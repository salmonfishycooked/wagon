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

var _ Queue = (*MemoryQueue)(nil)

// MemoryQueue is an in-memory queue.
// It's safe for concurrent use by multiple goroutines.
type MemoryQueue struct {
	items   []string
	pending []string

	mu sync.Mutex
}

func NewMemoryQueue() Queue {
	return &MemoryQueue{}
}

func (q *MemoryQueue) Enqueue(_ context.Context, taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.items = append(q.items, taskID)
	return nil
}

func (q *MemoryQueue) Dequeue(_ context.Context) (string, error) {
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

func (q *MemoryQueue) Ack(_ context.Context, taskID string) error {
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

func (q *MemoryQueue) Nack(_ context.Context, taskID string) error {
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
