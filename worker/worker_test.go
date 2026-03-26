package worker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/salmonfishycooked/wagon/queue"
	"github.com/salmonfishycooked/wagon/store"
	"github.com/salmonfishycooked/wagon/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ------------- necessary tools --------------

// default timeout of context.WithTimeout
var defaultTimeout = 3 * time.Second

func defaultQueue() queue.Queue {
	return queue.NewDefaultQueue()
}

func defaultStore() store.Store {
	return store.NewDefaultStore()
}

var successHandler Handler = func(_ context.Context, _ *task.Task) (result []byte, err error) {
	return []byte("ok"), nil
}

var failHandler Handler = func(_ context.Context, _ *task.Task) (result []byte, err error) {
	return nil, errors.New("error")
}

// ------------- tests ------------

func TestNewWorker(t *testing.T) {
	q, s := defaultQueue(), defaultStore()

	t.Run("normally construct a new DefaultWorker", func(t *testing.T) {
		worker, err := NewWorker(successHandler, q, s)
		require.NoError(t, err)
		require.NotNil(t, worker)
	})

	t.Run("lack of handler should return an error when constructing a new DefaultWorker", func(t *testing.T) {
		_, err := NewWorker(nil, q, s)
		require.Error(t, err)
	})

	t.Run("lack of queue should return an error when constructing a new DefaultWorker", func(t *testing.T) {
		_, err := NewWorker(successHandler, nil, s)
		require.Error(t, err)
	})

	t.Run("lack of store should return an error when constructing a new DefaultWorker", func(t *testing.T) {
		_, err := NewWorker(successHandler, q, nil)
		require.Error(t, err)
	})
}

func TestWithID(t *testing.T) {
	id := "my-id"
	q, s := defaultQueue(), defaultStore()

	worker, err := NewWorker(successHandler, q, s, WithID(id))
	require.NoError(t, err)
	require.NotNil(t, worker)

	defaultWorker := worker.(*DefaultWorker)
	require.Equal(t, id, defaultWorker.id)
}

func TestWithName(t *testing.T) {
	name := "my-name"
	q, s := defaultQueue(), defaultStore()

	worker, err := NewWorker(successHandler, q, s, WithName(name))
	require.NoError(t, err)
	require.NotNil(t, worker)

	defaultWorker := worker.(*DefaultWorker)
	require.Equal(t, name, defaultWorker.name)
}

func TestWithLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	q, s := defaultQueue(), defaultStore()

	worker, err := NewWorker(successHandler, q, s, WithLogger(logger))
	require.NoError(t, err)
	require.NotNil(t, worker)

	err = worker.Start(context.Background())
	require.NoError(t, err)

	err = worker.Shutdown(context.Background())
	require.NoError(t, err)

	require.True(t, strings.Contains(buf.String(), "worker"))
}

// TestWithRetryInterval tests whether the retryInterval works,
// which means that when the worker didn't take any task from the queue,
// the worker should be taking a nap with a period of retryInterval.
func TestWithRetryInterval(t *testing.T) {
	retryInterval := time.Hour
	q, s := defaultQueue(), defaultStore()

	worker, err := NewWorker(successHandler, q, s, WithRetryInterval(retryInterval))
	require.NoError(t, err)

	tsk := &task.Task{
		ID:         "task_id",
		Name:       "task_name",
		ScheduleAt: time.Now(),
	}

	ctx := context.Background()

	// ----------- test logic ------------
	synctest.Test(t, func(t *testing.T) {
		err := worker.Start(ctx)
		require.NoError(t, err)

		// initially, there's no any task in the queue,
		// so the worker will take a nap with a period of retryInterval.
		synctest.Wait()

		// after taking a nap with a period of retryInterval / 2,
		// the worker shouldn't be awake.
		time.Sleep(retryInterval / 2)

		// now we put a task into the queue, then sync all goroutines,
		// and the worker shouldn't retrieve the task from the queue.
		require.NoError(t, q.Enqueue(ctx, tsk.ID))
		require.NoError(t, s.Save(ctx, tsk))

		synctest.Wait()

		// now we try to dequeue from the queue, and we will receive a taskID,
		// instead of an error.
		_, err = q.Dequeue(ctx)
		require.NoError(t, err)

		// now enqueue the task again into the queue,
		// for being retrieved by worker after awake.
		require.NoError(t, q.Enqueue(ctx, tsk.ID))
		time.Sleep(retryInterval)

		// let the task finish the remaining things.
		synctest.Wait()

		// now make sure the queue is empty
		_, err = q.Dequeue(ctx)
		require.Error(t, err)

		// great, the worker passed the test :D
		// now, shutdown the worker to exit it.
		require.NoError(t, worker.Shutdown(ctx))

		synctest.Wait()
	})
}

func TestWorker(t *testing.T) {
	ctx := context.Background()

	t.Run("when panic occurs in handler, worker shouldn't crash", func(t *testing.T) {
		q, s := defaultQueue(), defaultStore()

		tsk := &task.Task{
			ID:         "task_id",
			Name:       "task_name",
			ScheduleAt: time.Now(),
		}
		require.NoError(t, q.Enqueue(ctx, tsk.ID))
		require.NoError(t, s.Save(ctx, tsk))

		var panicHandler Handler = func(_ context.Context, _ *task.Task) (result []byte, err error) {
			panic("oops")
		}

		worker, err := NewWorker(panicHandler, q, s)
		require.NoError(t, err)

		synctest.Test(t, func(t *testing.T) {
			// start the worker
			require.NoError(t, worker.Start(ctx))

			// waiting for that the worker successfully handled the panic task,
			// and sleep for the next task.
			//
			// if worker doesn't handle panic from handler, the test will fail here.
			synctest.Wait()

			// shutdown the worker
			assert.NoError(t, worker.Shutdown(ctx))

			synctest.Wait()
		})
	})
}
