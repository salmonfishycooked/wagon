package queue

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newQueue() Queue {
	return NewMemoryQueue()
}

// ---------------------------------------------------------------------------
// Enqueue / Dequeue basics
// ---------------------------------------------------------------------------

func TestEnqueueDequeue_SingleItem(t *testing.T) {
	q := newQueue()
	ctx := context.Background()

	require.NoError(t, q.Enqueue(ctx, "task-1"))

	id, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, "task-1", id)
}

func TestDequeue_EmptyQueue(t *testing.T) {
	q := newQueue()
	ctx := context.Background()

	_, err := q.Dequeue(ctx)
	require.ErrorIs(t, err, ErrEmpty)
}

func TestEnqueueDequeue_FIFO(t *testing.T) {
	q := newQueue()
	ctx := context.Background()

	ids := []string{"a", "b", "c", "d"}
	for _, id := range ids {
		require.NoError(t, q.Enqueue(ctx, id))
	}

	for _, want := range ids {
		got, err := q.Dequeue(ctx)
		require.NoError(t, err)
		assert.Equal(t, want, got, "FIFO order violated")
	}
}

func TestDequeue_EmptyAfterDraining(t *testing.T) {
	q := newQueue()
	ctx := context.Background()

	require.NoError(t, q.Enqueue(ctx, "only-one"))
	_, _ = q.Dequeue(ctx)

	_, err := q.Dequeue(ctx)
	require.ErrorIs(t, err, ErrEmpty)
}

// ---------------------------------------------------------------------------
// Ack
// ---------------------------------------------------------------------------

func TestAck_RemovesFromPending(t *testing.T) {
	q := newQueue()
	ctx := context.Background()

	require.NoError(t, q.Enqueue(ctx, "t1"))
	id, err := q.Dequeue(ctx)
	require.NoError(t, err)

	assert.NoError(t, q.Ack(ctx, id))
}

func TestAck_NonExistentTask(t *testing.T) {
	q := newQueue()
	ctx := context.Background()

	err := q.Ack(ctx, "ghost-task")
	require.ErrorIs(t, err, ErrNoSuchTask)
}

func TestAck_CannotAckTwice(t *testing.T) {
	q := newQueue()
	ctx := context.Background()

	require.NoError(t, q.Enqueue(ctx, "t1"))
	_, _ = q.Dequeue(ctx)
	require.NoError(t, q.Ack(ctx, "t1"))

	assert.ErrorIs(t, q.Ack(ctx, "t1"), ErrNoSuchTask)
}

func TestAck_ItemNotReenqueued(t *testing.T) {
	q := newQueue()
	ctx := context.Background()

	require.NoError(t, q.Enqueue(ctx, "t1"))
	_, _ = q.Dequeue(ctx)
	_ = q.Ack(ctx, "t1")

	_, err := q.Dequeue(ctx)
	require.ErrorIs(t, err, ErrEmpty, "acked task must not be re-dequeued")
}

// ---------------------------------------------------------------------------
// Nack
// ---------------------------------------------------------------------------

func TestNack_RequeuesTask(t *testing.T) {
	q := newQueue()
	ctx := context.Background()

	require.NoError(t, q.Enqueue(ctx, "t1"))
	_, _ = q.Dequeue(ctx)

	require.NoError(t, q.Nack(ctx, "t1"))

	id, err := q.Dequeue(ctx)
	require.NoError(t, err, "task should be back in queue after Nack")
	assert.Equal(t, "t1", id)
}

func TestNack_NonExistentTask(t *testing.T) {
	q := newQueue()
	ctx := context.Background()

	err := q.Nack(ctx, "ghost-task")
	require.ErrorIs(t, err, ErrNoSuchTask)
}

func TestNack_CannotNackTaskNotInPending(t *testing.T) {
	q := newQueue()
	ctx := context.Background()

	// Enqueue → Dequeue → Nack (re-enqueues) → Dequeue → Nack → Nack should fail.
	require.NoError(t, q.Enqueue(ctx, "t1"))
	_, _ = q.Dequeue(ctx)
	require.NoError(t, q.Nack(ctx, "t1")) // back in items

	_, _ = q.Dequeue(ctx)                 // move to pending
	require.NoError(t, q.Nack(ctx, "t1")) // back in items again

	// t1 is in items, NOT in pending — Nack must fail.
	err := q.Nack(ctx, "t1")
	require.ErrorIs(t, err, ErrNoSuchTask)
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

func TestConcurrent_EnqueueDequeue(t *testing.T) {
	q := newQueue()
	ctx := context.Background()

	const n = 100
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = q.Enqueue(ctx, string(rune('a'+i%26)))
		}(i)
	}
	wg.Wait()

	dequeued := 0
	for {
		_, err := q.Dequeue(ctx)
		if err != nil {
			break
		}
		dequeued++
	}
	assert.Equal(t, n, dequeued, "all enqueued items must be dequeued")
}

func TestConcurrent_AckNack(t *testing.T) {
	q := newQueue()
	ctx := context.Background()

	const n = 20
	for i := 0; i < n; i++ {
		require.NoError(t, q.Enqueue(ctx, string(rune('A'+i))))
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		id, err := q.Dequeue(ctx)
		if err != nil {
			break
		}
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if len(id)%2 == 0 {
				_ = q.Ack(ctx, id)
			} else {
				_ = q.Nack(ctx, id)
			}
		}(id)
	}
	wg.Wait()
	// No assertions needed — the test passes if there are no data races.
}
