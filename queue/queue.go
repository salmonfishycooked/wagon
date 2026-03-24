package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Queue is the task queue that only stores taskID of task.
// workers will retrieve task from Queue.
//
// All these method should be concurrency-safe.
//
// ⚠ [NOTE]:
//   - even though whatever method returns an error, it doesn't mean that Queue definitely didn't do
//     the corresponding action, due to the instability of network or something else.
//     for example, when someone called Ack with taskID, and received an error, this can't ensure that
//     the Queue didn't receive the ACK to the taskID.
//
// Sometimes, what we only need is just extra mechenism to handle the instability according to your demands. :)
type Queue interface {
	// Enqueue pushes a taskID into Queue.
	// any task in Queue will be taken by workers to execute.
	Enqueue(ctx context.Context, taskID string) error

	// Dequeue retrieves exactly one task from Queue.
	Dequeue(ctx context.Context) (taskID string, err error)

	// Ack reports the task with taskID to Queue that the task is done,
	// Whether the task is succeeded or eternally failed.
	// This operation will remove the task from Queue.
	Ack(ctx context.Context, taskID string) error

	// Nack reports the task with taskID to Queue that the task wasn't done by the worker (namely, caller),
	// and Queue will enqueue the task again.
	Nack(ctx context.Context, taskID string) error
}

var (
	ErrEmpty      = errors.New("queue is empty")
	ErrNoSuchTask = errors.New("no such task")
)

var _ Queue = (*defaultQueue)(nil)

// defaultQueue is an in-memory queue.
// It's safe for concurrent use by multiple goroutines.
type defaultQueue struct {
	items   []string
	pending []string

	mu sync.Mutex
}

func NewDefaultQueue() Queue {
	return &defaultQueue{}
}

func (q *defaultQueue) Enqueue(_ context.Context, taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.items = append(q.items, taskID)
	return nil
}

func (q *defaultQueue) Dequeue(_ context.Context) (string, error) {
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

func (q *defaultQueue) Ack(_ context.Context, taskID string) error {
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

func (q *defaultQueue) Nack(_ context.Context, taskID string) error {
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
