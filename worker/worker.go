package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/salmonfishycooked/wagon/queue"
	"github.com/salmonfishycooked/wagon/task"
)

// Worker defines all interfaces that it needs to be a standard worker.
type Worker interface {
	// Start starts the Worker.
	// It is non-blocking.
	// Do Start() before any method call to Worker.
	Start(ctx context.Context) error

	// Wait blocks until the Worker exit.
	Wait()

	// Shutdown emits gracefully shutdown，waiting for all goroutines being running done,
	// meanwhile it's also affected by ctx passed in.
	Shutdown(ctx context.Context) error
}

// Queue defines interfaces that Worker needs.
// See more interface detail in queue.Queue.
type Queue interface {
	Dequeue(ctx context.Context) (taskID string, err error)
	Ack(ctx context.Context, taskID string) error
	Nack(ctx context.Context, taskID string) error
}

// Store defines interfaces that Worker needs.
// See more interface detail in store.Store.
type Store interface {
	Get(ctx context.Context, taskID string) (*task.Task, error)
	UpdateStatus(ctx context.Context, taskID string, status task.Status, result []byte, detail string) error
}

// when there's no any task that can get from the task queue,
// the worker should sleep, and if outer didn't pass retry interval in,
// a worker should sleep the amount of defaultRetryInterval.
const defaultRetryInterval = time.Second

type Handler func(ctx context.Context, task *task.Task) (result []byte, err error)

// all possible states of DefaultWorker
const (
	StateInit int32 = iota
	StateRunning
	StateStopped
)

var _ Worker = (*DefaultWorker)(nil)

// DefaultWorker is the executor of tasks.
// It continuously retrieves task from the queue.
type DefaultWorker struct {
	// the id of me, whether it is unique depends on you.
	id string

	// the name of me,
	// please call me by a cutey name :D
	name string

	// the current state of me
	state atomic.Int32

	// wg is used to waiting for all goroutines managed by me done.
	wg sync.WaitGroup

	// Used to actively cancel the context of goroutines managed by me during shutdown.
	cancel context.CancelFunc

	// task queue which I finally get task waiting for being executed,
	// it must not be nil.
	queue Queue

	// database where I save and retrieve tasks information,
	// it must not be nil.
	store Store

	// handler defines how a task is executed.
	handler Handler

	// when I can't dequeue any task from queue, I should take a nap :D.
	// this can be 0, which means I will never take a nap.
	retryInterval time.Duration

	// logger can be nil, which means you don't need my working log.
	logger *slog.Logger
}

type config struct {
	id            string
	name          string
	retryInterval *time.Duration
	logger        *slog.Logger
}

func defaultConfig() *config {
	retryInterval := new(time.Duration)
	*retryInterval = defaultRetryInterval

	return &config{
		id:            uuid.NewString(),
		name:          "Holo Worker",
		retryInterval: retryInterval,
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

type Option func(w *config)

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

func WithRetryInterval(t time.Duration) Option {
	return func(cfg *config) {
		if t >= 0 {
			cfg.retryInterval = &t
		}
	}
}

// NewWorker needs non-nil handler, queue, store.
func NewWorker(handler Handler, queue Queue, store Store, opts ...Option) (Worker, error) {
	if handler == nil {
		return nil, errors.New("worker: handler must not be nil")
	}
	if queue == nil {
		return nil, errors.New("worker: queue must not be nil")
	}
	if store == nil {
		return nil, errors.New("worker: store must not be nil")
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	w := &DefaultWorker{
		id:            cfg.id,
		name:          cfg.name,
		state:         atomic.Int32{},
		wg:            sync.WaitGroup{},
		cancel:        nil,
		queue:         queue,
		store:         store,
		handler:       handler,
		retryInterval: *cfg.retryInterval,
		logger:        cfg.logger.With("component", "worker", "worker_id", cfg.id, "worker_name", cfg.name),
	}
	w.state.Store(StateInit)

	return w, nil
}

// Start makes the worker start to work.
// This method shouldn't block the goroutine.
func (w *DefaultWorker) Start(ctx context.Context) error {
	if !w.state.CompareAndSwap(StateInit, StateRunning) {
		return errors.New("worker: already started or stopped")
	}

	// Derive a context with cancellation to notify relevant goroutines during shutdown
	ctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	// start the working loop
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.run(ctx)
	}()

	return nil
}

// run doesn't return any error, which means it will handle all errors occurred.
// this will block the caller's goroutine.
func (w *DefaultWorker) run(ctx context.Context) {
	w.logger.Info("start working")

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("died")
			return
		default:
			taskID, err := w.queue.Dequeue(ctx)
			if errors.Is(err, queue.ErrEmpty) {
				w.logger.Debug("queue is empty, waiting for task", "retry_interval", w.retryInterval)
				time.Sleep(w.retryInterval)
				continue
			} else if err != nil {
				w.logger.Warn("dequeue a task from queue failed", "error", err, "retry_interval", w.retryInterval)
				time.Sleep(w.retryInterval)
				continue
			}

			w.process(ctx, taskID)
		}
	}
}

func (w *DefaultWorker) Wait() {
	w.wg.Wait()
}

func (w *DefaultWorker) Shutdown(ctx context.Context) error {
	if !w.state.CompareAndSwap(StateRunning, StateStopped) {
		return errors.New("worker: is not running")
	}

	// notify all goroutines managed by the scheduler ready to exit
	if w.cancel != nil {
		w.cancel()
	}

	// use channel to combine mechanisms of WaitGroup and Context,
	// if all relevant goroutines exit normally, done will notify the following select.
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.wg.Wait()
	}()

	select {
	case <-done:
		return nil // all relevant goroutines exit normally.
	case <-ctx.Done():
		return fmt.Errorf("worker: shutdown timeout: %w", ctx.Err())
	}
}

func (w *DefaultWorker) process(ctx context.Context, taskID string) {
	// in case of that worker panics.
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("panic occurred", "error", r)
		}
	}()

	tsk, err := w.store.Get(ctx, taskID)
	if err != nil {
		w.logger.Error("read task info from database failed", "error", err, "task_id", taskID)
		w.nack(ctx, taskID)
		return
	}

	err = w.store.UpdateStatus(ctx, taskID, task.StateRunning, nil, "")
	if err != nil {
		w.logger.Warn("update status to RUNNING failed", "task_id", taskID)
	}

	w.logger.Info("task is running", "task_id", taskID)
	result, err := w.handler(ctx, tsk)
	if err != nil {
		w.fail(ctx, tsk, err)
		return
	}

	w.success(ctx, tsk, result)
}

// success handles a task execution succeeded.
// It sets the task's status to Success and sends an ACK to the queue.
func (w *DefaultWorker) success(ctx context.Context, tsk *task.Task, result []byte) {
	w.logger.Info("task succeeded", "task_id", tsk.ID, "result", result)

	err := w.store.UpdateStatus(ctx, tsk.ID, task.StateSuccess, result, "")
	if err != nil {
		w.logger.Warn("update status to SUCCESS failed", "task_id", tsk.ID)
	}

	err = w.queue.Ack(ctx, tsk.ID)
	if err != nil {
		w.logger.Warn("ack to queue failed, task may be redelivered", "error", err, "task_id", tsk.ID)
	}
}

// fail handles a task execution failure.
// It resets the task's status to Pending and sends a NACK to the queue,
// so the task will be redelivered.
func (w *DefaultWorker) fail(ctx context.Context, tsk *task.Task, err error) {
	w.logger.Error("task failed", "task_id", tsk.ID, "error", err)
	err = w.store.UpdateStatus(ctx, tsk.ID, task.StatePending, nil, err.Error())
	if err != nil {
		w.logger.Error("update status to PENDING failed", "task_id", tsk.ID, "error", err)
	}
	w.nack(ctx, tsk.ID)
}

// nack sends NACK of task with taskID to worker's queue.
func (w *DefaultWorker) nack(ctx context.Context, taskID string) {
	err := w.queue.Nack(ctx, taskID)
	if err != nil {
		w.logger.Error("nack to queue failed, task may be redelivered or lost", "error", err, "task_id", taskID)
		return
	}

	w.logger.Debug("nack to queue succeeded, task will be redelivered", "task_id", taskID)
}
