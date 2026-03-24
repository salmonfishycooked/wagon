package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/salmonfishycooked/wagon/queue"
	"github.com/salmonfishycooked/wagon/store"
	"github.com/salmonfishycooked/wagon/task"
)

// test worker's handle functionality
func TestWorker_Integration_Success(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := queue.NewDefaultQueue()
	s := store.NewDefaultStore()

	tsk := &task.Task{ID: "t-success", Status: task.StatePending}
	if err := s.Save(ctx, tsk); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	if err := q.Enqueue(ctx, tsk.ID); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	handlerDone := make(chan struct{})
	handler := func(_ context.Context, _ *task.Task) ([]byte, error) {
		defer close(handlerDone)
		return []byte("ok"), nil
	}

	w, err := NewWorker(handler, q, s, WithRetryInterval(0))
	if err != nil {
		t.Fatalf("NewWorker failed: %v", err)
	}
	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("handler was not called within timeout")
	}

	// waiting for worker writing state of tasks done.
	time.Sleep(10 * time.Millisecond)

	got, err := s.Get(ctx, tsk.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.Status != task.StateSuccess {
		t.Errorf("want StateSuccess, got %v", got.Status)
	}
	if string(got.Result) != "ok" {
		t.Errorf("want result %q, got %q", "ok", got.Result)
	}
	if got.Reason != "" {
		t.Errorf("want empty reason on success, got %q", got.Reason)
	}
}

// mockStore will return an error when call Get
type mockStore struct {
	mu sync.Mutex

	getTask *task.Task
	getErr  error

	nackedIDs []string
}

func (m *mockStore) Get(_ context.Context, _ string) (*task.Task, error) {
	return m.getTask, m.getErr
}

func (m *mockStore) UpdateStatus(_ context.Context, taskID string, status task.Status, result []byte, reason string) error {
	return nil
}

type mockQueue struct {
	mu        sync.Mutex
	nackedIDs []string
}

func (m *mockQueue) Dequeue(_ context.Context) (string, error) {
	return "", queue.ErrEmpty
}

func (m *mockQueue) Ack(_ context.Context, _ string) error { return nil }

func (m *mockQueue) Nack(_ context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nackedIDs = append(m.nackedIDs, taskID)
	return nil
}

// when Store.Get failed, worker shouldn't proceed handling the task.
func TestWorker_StoreGetFails_NacksWithoutCallingHandler(t *testing.T) {
	ctx := context.Background()

	mq := &mockQueue{}
	ms := &mockStore{getErr: errors.New("db connection lost")}

	handlerCalled := false
	handler := func(_ context.Context, _ *task.Task) ([]byte, error) {
		handlerCalled = true
		return nil, nil
	}

	w, _ := NewWorker(handler, mq, ms)
	w.(*DefaultWorker).process(ctx, "t-store-fail")

	if handlerCalled {
		t.Error("handler must NOT be called when store.Get fails")
	}
	if len(mq.nackedIDs) != 1 || mq.nackedIDs[0] != "t-store-fail" {
		t.Errorf("expected Nack(t-store-fail), got %v", mq.nackedIDs)
	}
}

// if handler panics, worker shouldn't crash.
func TestWorker_HandlerPanic_WorkerSurvives(t *testing.T) {
	ctx := context.Background()

	mq := &mockQueue{}
	ms := &mockStore{getTask: &task.Task{ID: "t-panic"}}

	handler := func(_ context.Context, _ *task.Task) ([]byte, error) {
		panic("unexpected panic in handler")
	}

	w, _ := NewWorker(handler, mq, ms)

	w.(*DefaultWorker).process(ctx, "t-panic")

	// if the worker didn't crash, the test will pass.
}
