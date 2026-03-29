package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrEmpty      = errors.New("queue is empty")
	ErrNoSuchTask = errors.New("no such task")
)

var _ Queue = (*DefaultQueue)(nil)

// DefaultQueue is an in-memory queue.
// It's safe for concurrent use by multiple goroutines.
type DefaultQueue struct {
	items   []string
	pending []string

	mu sync.Mutex
}

func NewDefaultQueue() Queue {
	return &DefaultQueue{}
}

func (q *DefaultQueue) Enqueue(_ context.Context, taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.items = append(q.items, taskID)
	return nil
}

func (q *DefaultQueue) Dequeue(_ context.Context) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) == 0 {
		return "", ErrEmpty
	}

	taskID := q.items[0]
	q.items = q.items[1:]

	q.pending = append(q.pending, taskID)

	return taskID, nil
}

func (q *DefaultQueue) Ack(_ context.Context, taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for i, id := range q.pending {
		if id == taskID {
			q.pending = append(q.pending[:i], q.pending[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("%w: taskID %s doesn't exist", ErrNoSuchTask, taskID)
}

func (q *DefaultQueue) Nack(_ context.Context, taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for i, id := range q.pending {
		if id == taskID {
			q.pending = append(q.pending[:i], q.pending[i+1:]...)
			q.items = append(q.items, taskID)
			return nil
		}
	}

	return fmt.Errorf("%w: taskID %s doesn't exist", ErrNoSuchTask, taskID)
}

// DefaultConnector is the connector that belongs to DefaultQueue.
type DefaultConnector struct{}

func NewDefaultConnector() Connector {
	return &DefaultConnector{}
}

func (c *DefaultConnector) Connect(_ context.Context) (Queue, error) {
	return NewDefaultQueue(), nil
}
