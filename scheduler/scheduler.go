package scheduler

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/salmonfishycooked/wagon/task"
)

// Scheduler defines interfaces that a standard scheduler needs.
//
// any specific Scheduler shouldn't be source of getting task information,
// it can only store the copy of task information.
type Scheduler interface {
	// Start makes the scheduler start to work.
	// It is non-blocking.
	// Do Start() before any method call to Scheduler.
	Start(ctx context.Context) error

	// Wait blocks until scheduler exits.
	Wait()

	// Shutdown emits gracefully shutdown，waiting for the scheduler exiting,
	// meanwhile it's also affected by ctx passed in.
	Shutdown(ctx context.Context) error

	// Submit submits a task waiting for being scheduled.
	Submit(ctx context.Context, tsk *task.Task) error
}

// Queue defines interfaces that Scheduler needs.
// See more interface detail in queue.Queue.
type Queue interface {
	Enqueue(ctx context.Context, taskID string) error
}

// Store defines interfaces that Scheduler needs.
// See more interface detail in store.Store.
type Store interface {
	Get(ctx context.Context, taskID string) (*task.Task, error)
	Save(ctx context.Context, tsk *task.Task) error
}

// all possible states of DefaultScheduler
const (
	StateInit int32 = iota
	StateRunning
	StateStopped
)

// if there's no any task in dq, we can't calculate out the next nap time of the scheduler,
// so we task defaultNapTime.
const defaultNapTime = time.Minute

type DefaultScheduler struct {
	// the id of me, whether it is unique depends on you.
	id string

	// the name of me,
	// please call me by a cutey name :D
	name string

	// the current state of me
	state atomic.Int32

	// wg is used to waiting for all other components done (like dq).
	wg sync.WaitGroup

	// Used to actively cancel the context of goroutines managed by me during shutdown.
	cancel context.CancelFunc

	// task queue which I finally pushes due tasks into,
	// it must not be nil.
	queue Queue

	// database where I save and retrieve tasks information,
	// it must not be nil.
	store Store

	// dq is used to delay task whose schedule time doesn't arrive.
	// the task will pop out from dq when its scheduler time arrives.
	dq delayQueue

	// dqMu protects dq.
	// dq is not safe for concurrent use.
	dqMu sync.Mutex

	// wakeUp interrupts my current sleep early to trigger an immediate
	// dispatch. It is signaled when a newly submitted task is due sooner than the
	// top task in dq, or when dq is empty.
	wakeUp chan struct{}

	// logger can be nil, which means you don't need my working log.
	logger *slog.Logger
}

type config struct {
	id     string
	name   string
	logger *slog.Logger
}

func defaultConfig() *config {
	return &config{
		id:     uuid.NewString(),
		name:   "Holo Scheduler",
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

type Option func(s *config)

func WithID(id string) Option {
	return func(cfg *config) {
		if id != "" {
			cfg.id = id
		}
	}
}

func WithName(name string) Option {
	return func(cfg *config) {
		if name != "" {
			cfg.name = name
		}
	}
}

func WithLogger(logger *slog.Logger) Option {
	return func(cfg *config) {
		if logger != nil {
			cfg.logger = logger
		}
	}
}

// NewDefaultScheduler needs non-nil queue and store.
func NewDefaultScheduler(queue Queue, store Store, opts ...Option) (Scheduler, error) {
	if queue == nil {
		return nil, errors.New("scheduler: queue must not be nil")
	}
	if store == nil {
		return nil, errors.New("scheduler: store must not be nil")
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	s := &DefaultScheduler{
		id:     cfg.id,
		name:   cfg.name,
		state:  atomic.Int32{},
		wg:     sync.WaitGroup{},
		cancel: nil,
		queue:  queue,
		store:  store,
		dq:     delayQueue{},
		dqMu:   sync.Mutex{},
		wakeUp: make(chan struct{}, 1), // cap set to 1, in case of blocking sender
		logger: cfg.logger.With("component", "scheduler", "scheduler_id", cfg.id, "scheduler_name", cfg.name),
	}
	s.state.Store(StateInit)

	return s, nil
}

func (s *DefaultScheduler) Start(ctx context.Context) error {
	if !s.state.CompareAndSwap(StateInit, StateRunning) {
		return errors.New("scheduler: already started or stopped")
	}

	// Derive a context with cancellation to notify relevant goroutines during shutdown
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	// start the working loop
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.run(ctx)
	}()

	return nil
}

// run doesn't return any error, which means it will handle all errors occurred.
// this will block the caller's goroutine.
func (s *DefaultScheduler) run(ctx context.Context) {
	s.logger.Info("start working")
	for {
		napTime := s.dispatch(ctx)
		select {
		case <-ctx.Done():
			s.logger.Info("died")
			return
		case <-time.After(napTime):
		case <-s.wakeUp:
		}
	}
}

func (s *DefaultScheduler) Wait() {
	s.wg.Wait()
}

func (s *DefaultScheduler) Shutdown(ctx context.Context) error {
	if !s.state.CompareAndSwap(StateRunning, StateStopped) {
		return errors.New("scheduler: is not running")
	}

	// notify all goroutines managed by the scheduler ready to exit
	if s.cancel != nil {
		s.cancel()
	}

	// use channel to combine mechanisms of WaitGroup and Context,
	// if all relevant goroutines exit normally, done will notify the following select.
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.wg.Wait()
	}()

	select {
	case <-done:
		return nil // all relevant goroutines exit normally.
	case <-ctx.Done():
		return fmt.Errorf("scheduler: shutdown timeout: %w", ctx.Err())
	}
}

// dispatch enqueues all tasks in dq whose schedule time has arrived into queue.
// It returns napTime = (nextScheduleTime - now), indicating how long the scheduler
// should sleep before the next dispatch, where nextScheduleTime is the schedule
// time of the current top task in dq.
func (s *DefaultScheduler) dispatch(ctx context.Context) (napTime time.Duration) {
	now := time.Now()
	var tasks []*task.Task

	// pop due tasks
	s.dqMu.Lock()
	for len(s.dq.items) > 0 && now.After(s.dq.items[0].ScheduleTime()) {
		tsk := heap.Pop(&s.dq).(*task.Task)
		tasks = append(tasks, tsk)
	}
	// calculate the next napTime
	napTime = defaultNapTime
	if len(s.dq.items) > 0 {
		napTime = s.dq.items[0].ScheduleTime().Sub(now)
	}
	s.dqMu.Unlock()
	// unlock dq before here, as following operations may be slow (like i/o operation).

	// enqueue all due tasks into queue
	for _, tsk := range tasks {
		if err := s.queue.Enqueue(ctx, tsk.ID); err != nil {
			s.logger.Error("enqueue task into task queue failed, task may be in task queue or not", "task_id", tsk.ID)
			continue
		}
		s.logger.Debug("task is sent to task queue", "task_id", tsk.ID)
	}

	return napTime
}

// Submit submits the task into delay queue.
// Process:
//   - 1. save the task into store (make sure the status of the task is task.StatePending)
//     if failed, return.
//   - 2. push the task into delay queue.
//   - 3. check if the top of the delay queue is the new task, which means we need to wake up the scheduler
//     by sending data into s.wakeUp, for reducing unnecessary latency of the task.
func (s *DefaultScheduler) Submit(ctx context.Context, tsk *task.Task) error {
	// make sure the state is Running
	if s.state.Load() != StateRunning {
		s.logger.Error("submit task when current state is not Running")
		return errors.New("scheduler: submit task when current state is not Running")
	}

	// make a copy, in case of modifying by outer.
	tskCopy := &task.Task{}
	*tskCopy = *tsk

	// status of task submitted by users should be Pending.
	tskCopy.Status = task.StatePending

	// save the task information into store
	err := s.store.Save(ctx, tskCopy)
	if err != nil {
		s.logger.Error("save task to database failed, task may be in database or not", "error", err, "task_id", tsk.ID)
		return err
	}

	s.dqMu.Lock()
	heap.Push(&s.dq, tskCopy)
	// we need to wake up the scheduler, if there's already a signal in wakeUp channel,
	// we do not need to put again.
	if s.dq.items[0] == tskCopy && len(s.wakeUp) == 0 {
		s.logger.Debug("the new task is ahead of the top of the delay queue, wake up the scheduler to start working")
		s.wakeUp <- struct{}{}
	}
	s.dqMu.Unlock()

	s.logger.Debug("task is submitted, waiting for getting into the task queue", "task_id", tskCopy.ID)

	return nil
}

type delayQueue struct {
	items []*task.Task
}

func (q *delayQueue) Len() int { return len(q.items) }
func (q *delayQueue) Less(i, j int) bool {
	return q.items[i].ScheduleTime().Before(q.items[j].ScheduleTime())
}
func (q *delayQueue) Swap(i, j int) { q.items[i], q.items[j] = q.items[j], q.items[i] }
func (q *delayQueue) Push(x any) {
	t := x.(*task.Task)
	q.items = append(q.items, t)
}
func (q *delayQueue) Pop() any {
	if q.Len() <= 0 {
		return nil
	}
	t := q.items[q.Len()-1]
	q.items[q.Len()-1] = nil
	q.items = q.items[:q.Len()-1]

	return t
}
