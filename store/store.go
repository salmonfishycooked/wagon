package store

import (
	"context"

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
