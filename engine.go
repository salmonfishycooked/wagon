package wagon

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/salmonfishycooked/wagon/scheduler"
	"github.com/salmonfishycooked/wagon/task"
	"github.com/salmonfishycooked/wagon/worker/pool"
)

type Engine interface {
	// Start starts the engine.
	// It is non-blocking.
	// Do Start() before any method call to Engine.
	Start(ctx context.Context) error

	// Wait blocks until all goroutines managed by Engine exits.
	Wait()

	// Shutdown emits gracefully shutdown，waiting for the Engine exiting,
	// meanwhile it's also affected by ctx passed in.
	Shutdown(ctx context.Context) error

	// Submit submits a task waiting for being scheduled.
	Submit(ctx context.Context, tsk *task.Task) error
}

// all possible states of defaultEngine
const (
	StateInit int32 = iota
	StateRunning
	StateStopped
)

var _ Engine = (*defaultEngine)(nil)

type defaultEngine struct {
	// sched is the task scheduler
	sched scheduler.Scheduler

	// workerPool manages all workers
	workerPool pool.Pool

	// the current state of me
	state atomic.Int32

	// wg is used to waiting for all goroutines managed by me done.
	wg sync.WaitGroup

	// Used to actively cancel the context of goroutines managed by me during shutdown.
	cancel context.CancelFunc
}

// New returns a defaultEngine.
// handler must not be nil.
func New(sched scheduler.Scheduler, workerPool pool.Pool) (Engine, error) {
	if sched == nil {
		return nil, errors.New("engine: scheduler must not be nil")
	}
	if workerPool == nil {
		return nil, errors.New("engine: worker pool must not be nil")
	}

	e := &defaultEngine{
		sched:      sched,
		workerPool: workerPool,
		state:      atomic.Int32{},
		wg:         sync.WaitGroup{},
		cancel:     nil,
	}
	e.state.Store(StateInit)

	return e, nil
}

func (e *defaultEngine) Start(ctx context.Context) error {
	if !e.state.CompareAndSwap(StateInit, StateRunning) {
		return errors.New("engine: already started or stopped")
	}

	// Derive a context with cancellation to notify relevant goroutines during shutdown
	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel

	// start scheduler and worker pool
	if err := e.sched.Start(ctx); err != nil {
		return err
	}
	if err := e.workerPool.Start(ctx); err != nil {
		cancel()
		return err
	}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.sched.Wait()
	}()

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.workerPool.Wait()
	}()

	return nil
}

func (e *defaultEngine) Wait() {
	e.wg.Wait()
}

func (e *defaultEngine) Shutdown(ctx context.Context) error {
	if !e.state.CompareAndSwap(StateRunning, StateStopped) {
		return errors.New("engine: is not running")
	}

	// notify all goroutines managed by the scheduler ready to exit
	if e.cancel != nil {
		e.cancel()
	}

	// use channel to combine mechanisms of WaitGroup and Context,
	// if all relevant goroutines exit normally, done will notify the following select.
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.wg.Wait()
	}()

	select {
	case <-done:
		return nil // all relevant goroutines exit normally.
	case <-ctx.Done():
		return fmt.Errorf("engine: shutdown timeout: %w", ctx.Err())
	}
}

func (e *defaultEngine) Submit(ctx context.Context, tsk *task.Task) error {
	if e.state.Load() != StateRunning {
		return errors.New("engine: submit task when current state is not Running")
	}

	return e.sched.Submit(ctx, tsk)
}
