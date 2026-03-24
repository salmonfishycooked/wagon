package pool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/salmonfishycooked/wagon/worker"
)

// Pool defines all interfaces that it needs to be a standard worker pool.
type Pool interface {
	// Start starts all workers.
	// It is non-blocking.
	// Do Start() before any method call to Pool.
	Start(ctx context.Context) error

	// Wait blocks until all workers exit.
	Wait()

	// Shutdown emits gracefully shutdown，waiting for all goroutines being running done,
	// meanwhile it's also affected by ctx passed in.
	Shutdown(ctx context.Context) error
}

// all possible states of DefaultPool
const (
	StateInit int32 = iota
	StateRunning
	StateStopped
)

// NameFunc generates a display name for the i-th worker in a pool.
type NameFunc func(index int) string

func defaultNameFunc(index int) string {
	return fmt.Sprintf("worker-%d", index)
}

// IDFunc generates an id for the i-th worker in a pool.
type IDFunc func(index int) string

func defaultIDFunc(index int) string {
	return fmt.Sprintf("worker-%d-%s", index, uuid.NewString())
}

var _ Pool = (*DefaultPool)(nil)

// DefaultPool holds a fixed set of workers that all share the same queue and store.
type DefaultPool struct {
	workers []worker.Worker

	// the current state of me
	state atomic.Int32

	// wg is used to waiting for all goroutines managed by me done.
	wg sync.WaitGroup

	// Used to actively cancel the context of goroutines managed by me during shutdown.
	cancel context.CancelFunc

	// logger can be nil, which means you don't need my working log.
	logger *slog.Logger
}

// config collects option values before the DefaultPool is built.
type config struct {
	size          int
	nameFn        NameFunc
	idFunc        IDFunc
	retryInterval *time.Duration
	logger        *slog.Logger
}

func defaultConfig() *config {
	return &config{
		size:          runtime.NumCPU(),
		nameFn:        defaultNameFunc,
		idFunc:        defaultIDFunc,
		retryInterval: nil,
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

type Option func(cfg *config)

func WithSize(size int) Option {
	return func(cfg *config) {
		if size > 0 {
			cfg.size = size
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

func WithNameFunc(fn NameFunc) Option {
	return func(cfg *config) {
		if fn != nil {
			cfg.nameFn = fn
		}
	}
}

func WithIDFunc(fn IDFunc) Option {
	return func(cfg *config) {
		if fn != nil {
			cfg.idFunc = fn
		}
	}
}

func WithRetryInterval(interval time.Duration) Option {
	return func(cfg *config) {
		if interval >= 0 {
			cfg.retryInterval = &interval
		}
	}
}

func NewDefaultPool(handler worker.Handler, queue worker.Queue, store worker.Store, opts ...Option) (Pool, error) {
	if handler == nil {
		return nil, errors.New("worker pool: handler must not be nil")
	}
	if queue == nil {
		return nil, errors.New("worker pool: queue must not be nil")
	}
	if store == nil {
		return nil, errors.New("worker pool: store must not be nil")
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	p := &DefaultPool{
		workers: make([]worker.Worker, cfg.size),
		logger:  cfg.logger.With("component", "worker pool"),
	}

	for i := range p.workers {
		workerID := cfg.idFunc(i)
		workerName := cfg.nameFn(i)

		workerOpts := []worker.Option{
			worker.WithID(workerID),
			worker.WithName(workerName),
			worker.WithLogger(cfg.logger),
		}
		if cfg.retryInterval != nil {
			workerOpts = append(workerOpts, worker.WithRetryInterval(*cfg.retryInterval))
		}

		newWorker, err := worker.NewWorker(handler, queue, store, workerOpts...)
		if err != nil {
			return nil, err
		}
		p.workers[i] = newWorker
	}
	p.state.Store(StateInit)

	return p, nil
}

func (p *DefaultPool) Start(ctx context.Context) error {
	if !p.state.CompareAndSwap(StateInit, StateRunning) {
		return errors.New("pool: already started or stopped")
	}

	// Derive a context with cancellation to notify workers during shutdown
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	for _, w := range p.workers {
		p.wg.Add(1)
		go func(wk worker.Worker) {
			defer p.wg.Done()
			if err := wk.Start(ctx); err != nil {
				p.logger.Error("start worker failed", "error", err)
				return
			}

			wk.Wait()
		}(w)
	}

	return nil
}

func (p *DefaultPool) Wait() {
	p.wg.Wait()
}

func (p *DefaultPool) Shutdown(ctx context.Context) error {
	if !p.state.CompareAndSwap(StateRunning, StateStopped) {
		return errors.New("pool: is not running")
	}

	// notify all workers ready to exit
	if p.cancel != nil {
		p.cancel()
	}

	// use channel to combine mechanisms of WaitGroup and Context,
	// if all workers exit normally, done will notify the following select.
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.wg.Wait()
	}()

	select {
	case <-done:
		return nil // all workers exit normally.
	case <-ctx.Done():
		return fmt.Errorf("pool: shutdown timeout: %w", ctx.Err())
	}
}
