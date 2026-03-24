package store_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/salmonfishycooked/wagon/store"
	"github.com/salmonfishycooked/wagon/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newStore() store.Store {
	return store.NewDefaultStore()
}

func sampleTask(id string) *task.Task {
	return &task.Task{
		ID:         id,
		Name:       "test-task",
		Status:     task.StatePending,
		ScheduleAt: time.Now(),
		Payload:    []byte("payload"),
	}
}

// ---------------------------------------------------------------------------
// Save
// ---------------------------------------------------------------------------

func TestSave_NewTask(t *testing.T) {
	s := newStore()
	ctx := context.Background()

	require.NoError(t, s.Save(ctx, sampleTask("id-1")))
}

func TestSave_DuplicateID(t *testing.T) {
	s := newStore()
	ctx := context.Background()

	tsk := sampleTask("id-dup")
	require.NoError(t, s.Save(ctx, tsk))

	err := s.Save(ctx, tsk)
	require.ErrorIs(t, err, store.ErrTaskAlreadyExist)
}

func TestSave_IsolatesCopy(t *testing.T) {
	// Mutating the original after Save must not affect the stored value.
	s := newStore()
	ctx := context.Background()

	tsk := sampleTask("id-iso")
	require.NoError(t, s.Save(ctx, tsk))

	tsk.Name = "mutated"

	got, err := s.Get(ctx, "id-iso")
	require.NoError(t, err)
	assert.NotEqual(t, "mutated", got.Name, "Save must store a copy, not a reference")
}

// ---------------------------------------------------------------------------
// Get
// ---------------------------------------------------------------------------

func TestGet_ExistingTask(t *testing.T) {
	s := newStore()
	ctx := context.Background()

	require.NoError(t, s.Save(ctx, sampleTask("id-get")))

	got, err := s.Get(ctx, "id-get")
	require.NoError(t, err)
	assert.Equal(t, "id-get", got.ID)
}

func TestGet_NonExistent(t *testing.T) {
	s := newStore()
	ctx := context.Background()

	_, err := s.Get(ctx, "does-not-exist")
	require.ErrorIs(t, err, store.ErrNoSuchTask)
}

func TestGet_ReturnsCopy(t *testing.T) {
	// Mutating the returned task must not affect the stored value.
	s := newStore()
	ctx := context.Background()

	require.NoError(t, s.Save(ctx, sampleTask("id-copy")))

	got, err := s.Get(ctx, "id-copy")
	require.NoError(t, err)
	got.Name = "tampered"

	fresh, err := s.Get(ctx, "id-copy")
	require.NoError(t, err)
	assert.NotEqual(t, "tampered", fresh.Name, "Get must return a copy, not the internal reference")
}

// ---------------------------------------------------------------------------
// UpdateStatus
// ---------------------------------------------------------------------------

func TestUpdateStatus_Success(t *testing.T) {
	s := newStore()
	ctx := context.Background()

	require.NoError(t, s.Save(ctx, sampleTask("id-upd")))
	require.NoError(t, s.UpdateStatus(ctx, "id-upd", task.StateSuccess, []byte("ok"), ""))

	got, err := s.Get(ctx, "id-upd")
	require.NoError(t, err)
	assert.Equal(t, task.StateSuccess, got.Status)
	assert.Equal(t, []byte("ok"), got.Result)
	assert.Empty(t, got.Reason)
}

func TestUpdateStatus_Failure(t *testing.T) {
	s := newStore()
	ctx := context.Background()

	require.NoError(t, s.Save(ctx, sampleTask("id-fail")))
	require.NoError(t, s.UpdateStatus(ctx, "id-fail", task.StateFailed, nil, "something went wrong"))

	got, err := s.Get(ctx, "id-fail")
	require.NoError(t, err)
	assert.Equal(t, task.StateFailed, got.Status)
	assert.Equal(t, "something went wrong", got.Reason)
	assert.Nil(t, got.Result)
}

func TestUpdateStatus_NilResultOverwritesPrevious(t *testing.T) {
	s := newStore()
	ctx := context.Background()

	require.NoError(t, s.Save(ctx, sampleTask("id-nil")))
	require.NoError(t, s.UpdateStatus(ctx, "id-nil", task.StateSuccess, []byte("first"), ""))
	require.NoError(t, s.UpdateStatus(ctx, "id-nil", task.StateFailed, nil, "oops"))

	got, err := s.Get(ctx, "id-nil")
	require.NoError(t, err)
	assert.Nil(t, got.Result, "nil result should overwrite previous result")
}

func TestUpdateStatus_NonExistentTask(t *testing.T) {
	s := newStore()
	ctx := context.Background()

	err := s.UpdateStatus(ctx, "ghost", task.StateSuccess, nil, "")
	require.ErrorIs(t, err, store.ErrNoSuchTask)
}

func TestUpdateStatus_AllStatusTransitions(t *testing.T) {
	transitions := []task.Status{
		task.StatePending,
		task.StateRunning,
		task.StateSuccess,
		task.StateFailed,
	}

	for _, status := range transitions {
		status := status
		t.Run("", func(t *testing.T) {
			s := newStore()
			ctx := context.Background()

			require.NoError(t, s.Save(ctx, sampleTask("id-trans")))
			require.NoError(t, s.UpdateStatus(ctx, "id-trans", status, nil, ""))

			got, err := s.Get(ctx, "id-trans")
			require.NoError(t, err)
			assert.Equal(t, status, got.Status)
		})
	}
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

func TestConcurrent_SaveAndGet(t *testing.T) {
	s := newStore()
	ctx := context.Background()

	const n = 50
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := string(rune('a' + i%26))
			// Ignore duplicate errors — they're expected for colliding IDs.
			_ = s.Save(ctx, &task.Task{ID: id, Name: "concurrent"})
		}(i)
	}
	wg.Wait()
	// Test passes if there are no data races.
}
