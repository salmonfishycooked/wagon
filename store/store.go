package store

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/salmonfishycooked/wagon/task"
)

// Store is the only data source of tasks.
// It has information of all tasks.
//
// All these method should be concurrency-safe.
//
// ⚠ [NOTE]:
//   - even though whatever method returns an error, it doesn't mean that Store definitely didn't do
//     the corresponding action, due to the instability of network or something else.
//     for example, when someone called Save with a task entity, and received an error, this can't ensure that
//     the Store didn't save the task into its database.
//
// Sometimes, what we only need is just extra mechenism to handle the instability according to your demands. :)
type Store interface {
	// Get returns full information of task with taskID.
	// if there's no existing task with taskID, then an error will be returned.
	Get(ctx context.Context, taskID string) (*task.Task, error)

	// Save stores a new task.
	// if there's existing task that has the same taskID, it should return an error.
	Save(ctx context.Context, tsk *task.Task) error

	// UpdateStatus updates the status of task with taskID.
	//
	// whether result or reason is zero-value, they will always be overwritten.
	// like, if result is nil, then the result of task with taskID in database will be empty too.
	UpdateStatus(ctx context.Context, taskID string, status task.Status, result []byte, reason string) error
}

var (
	ErrNoSuchTask       = errors.New("no such task")
	ErrTaskAlreadyExist = errors.New("the task already exists")
)

// defaultStore is an in-memory storage.
// It's safe for concurrent use by multiple goroutines.
type defaultStore struct {
	items map[string]*task.Task

	mu sync.Mutex
}

func NewDefaultStore() Store {
	return &defaultStore{items: make(map[string]*task.Task)}
}

func (s *defaultStore) Get(_ context.Context, taskID string) (*task.Task, error) {
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

func (s *defaultStore) Save(_ context.Context, tsk *task.Task) error {
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

func (s *defaultStore) UpdateStatus(_ context.Context, taskID string, status task.Status, result []byte, reason string) error {
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
