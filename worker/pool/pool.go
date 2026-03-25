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

	// newWorker defines how I create a new worker.
	newWorker func() (worker.Worker, error)

	// logger can be nil, which means you don't need my working log.
	logger *slog.Logger
}

// config collects option values before the DefaultPool is built.
type config struct {
	size   int
	logger *slog.Logger
}

func defaultConfig() *config {
	return &config{
		size:   runtime.NumCPU(),
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
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

func NewDefaultPool(newWorker func() (worker.Worker, error), opts ...Option) (Pool, error) {
	if newWorker == nil {
		return nil, errors.New("worker pool: newWorker must not be nil")
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	p := &DefaultPool{
		workers:   make([]worker.Worker, cfg.size),
		newWorker: newWorker,
		logger:    cfg.logger.With("component", "worker pool"),
	}

	for i := range p.workers {
		w, err := p.newWorker()
		if err != nil {
			return nil, fmt.Errorf("pool: create worker[%d] failed: %w", i, err)
		}
		p.workers[i] = w
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
