package queue

import (
	"context"
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
