package task

import "time"

// Status represents the current status of task.
type Status int32

const (
	// StatePending means task is submitted, but the schedule time doesn't arrive,
	// or the schedule time arrived, but the task hasn't been executed by any worker.
	StatePending Status = iota

	// StateRunning means task is being executed by a worker.
	StateRunning

	// StateSuccess means task is finished and succeeded.
	StateSuccess

	// StateFailed means task is finished and failed eternally.
	StateFailed
)

type Task struct {
	// ID is unique identifier of the task.
	ID string

	Name string

	Status Status

	// ScheduleAt is the intended execution time of the task,
	// namely when the worker should start running it.
	ScheduleAt time.Time

	// DispatchAhead is how far before ScheduleAt the task should be enqueued
	// so that a worker can pick it up and be ready to execute exactly on time.
	//
	// Actual enqueue time = ScheduleAt - DispatchAhead.
	// A zero value means the task is enqueued at ScheduleAt (no advance dispatch).
	DispatchAhead time.Duration

	// Payload will be taken as the argument to your worker.Handler
	Payload []byte

	// Result is the message produced by the worker.Handler
	Result []byte

	// When the task is failed, Reason will be filled with reason of failing,
	// otherwise Reason should be empty string.
	Reason string
}

// ScheduleTime returns the time at which the task should be placed onto the queue.
// It's equivalent to ScheduleAt minus DispatchAhead.
func (t *Task) ScheduleTime() time.Time {
	return t.ScheduleAt.Add(-t.DispatchAhead)
}
