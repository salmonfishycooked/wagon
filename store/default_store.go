package store

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/salmonfishycooked/wagon/task"
)

var (
	ErrNoSuchTask       = errors.New("no such task")
	ErrTaskAlreadyExist = errors.New("the task already exists")
)

// DefaultStore is an in-memory storage.
// It's safe for concurrent use by multiple goroutines.
type DefaultStore struct {
	items map[string]*task.Task

	mu sync.Mutex
}

func NewDefaultStore() Store {
	return &DefaultStore{items: make(map[string]*task.Task)}
}

func (s *DefaultStore) Get(_ context.Context, taskID string) (*task.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.items[taskID]
	if !ok {
		return nil, fmt.Errorf("%w: taskID %s doesn't exist", ErrNoSuchTask, taskID)
	}

	// make a copy, in case of being modified accidentally by outer.
	t := *v
	return &t, nil
}

func (s *DefaultStore) Save(_ context.Context, tsk *task.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.items[tsk.ID]
	if ok {
		return fmt.Errorf("%w: with taskID %s", ErrTaskAlreadyExist, tsk.ID)
	}

	// make a copy, in case of being modified accidentally by outer.
	t := *tsk
	s.items[t.ID] = &t
	return nil
}

func (s *DefaultStore) UpdateStatus(_ context.Context, taskID string, status task.Status, result []byte, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.items[taskID]
	if !ok {
		return fmt.Errorf("%w: taskID %s doesn't exist", ErrNoSuchTask, taskID)
	}

	v.Status = status
	v.Result = result
	v.Reason = reason

	return nil
}

// DefaultConnector is the connector that belongs to DefaultStore.
type DefaultConnector struct{}

func NewDefaultConnector() Connector {
	return &DefaultConnector{}
}

func (c *DefaultConnector) Connect(_ context.Context) (Store, error) {
	return NewDefaultStore(), nil
}
