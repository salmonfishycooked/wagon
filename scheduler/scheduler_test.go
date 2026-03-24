package scheduler

// go test ./scheduler/... -v -race

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/salmonfishycooked/wagon/task"
)

// ----------------------------------------------------------------
// Mocks
// ----------------------------------------------------------------

type mockQueue struct {
	mu          sync.Mutex
	enqueuedIDs []string
	enqueueErr  error
}

func (m *mockQueue) Enqueue(_ context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.enqueueErr != nil {
		return m.enqueueErr
	}
	m.enqueuedIDs = append(m.enqueuedIDs, taskID)
	return nil
}

func (m *mockQueue) ids() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]string, len(m.enqueuedIDs))
	copy(cp, m.enqueuedIDs)
	return cp
}

type mockStore struct {
	mu      sync.Mutex
	saved   []*task.Task
	saveErr error
}

func (m *mockStore) Get(_ context.Context, _ string) (*task.Task, error) { return nil, nil }

func (m *mockStore) Save(_ context.Context, tsk *task.Task) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *tsk
	m.saved = append(m.saved, &cp)
	return nil
}

// countingQueue fails on the Nth Enqueue call, succeeds otherwise.
type countingQueue struct {
	mu          sync.Mutex
	callCount   int
	failOn      int
	enqueuedIDs []string
}

func (c *countingQueue) Enqueue(_ context.Context, taskID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.callCount++
	if c.callCount == c.failOn {
		return errors.New("enqueue failed")
	}
	c.enqueuedIDs = append(c.enqueuedIDs, taskID)
	return nil
}

// ----------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------

func makeScheduler(t *testing.T, q Queue, s Store) *DefaultScheduler {
	t.Helper()
	sched, err := NewDefaultScheduler(q, s)
	if err != nil {
		t.Fatalf("NewDefaultScheduler: %v", err)
	}
	return sched.(*DefaultScheduler)
}

func makeTask(id string, scheduleAt time.Time) *task.Task {
	return &task.Task{ID: id, ScheduleAt: scheduleAt}
}

// pushToDQ writes tasks directly into dq, bypassing Submit's store/wakeUp side effects.
// Use this to isolate dispatch() tests.
func pushToDQ(s *DefaultScheduler, tasks ...*task.Task) {
	s.dqMu.Lock()
	defer s.dqMu.Unlock()
	for _, tsk := range tasks {
		heap.Push(&s.dq, tsk)
	}
}

// ----------------------------------------------------------------
// delayQueue
// ----------------------------------------------------------------

func TestDelayQueue_OrdersByScheduleTime(t *testing.T) {
	now := time.Now()
	dq := &delayQueue{}

	heap.Push(dq, &task.Task{ID: "t3", ScheduleAt: now.Add(3 * time.Second)})
	heap.Push(dq, &task.Task{ID: "t1", ScheduleAt: now.Add(1 * time.Second)})
	heap.Push(dq, &task.Task{ID: "t2", ScheduleAt: now.Add(2 * time.Second)})

	for i, wantID := range []string{"t1", "t2", "t3"} {
		got := heap.Pop(dq).(*task.Task)
		if got.ID != wantID {
			t.Errorf("pop[%d]: want %s, got %s", i, wantID, got.ID)
		}
	}
}

func TestDelayQueue_DispatchAhead_AffectsOrder(t *testing.T) {
	// t1: ScheduleAt=now+10s, DispatchAhead=9s => ScheduleTime=now+1s
	// t2: ScheduleAt=now+2s,  DispatchAhead=0  => ScheduleTime=now+2s
	// t1 should pop before t2
	now := time.Now()
	dq := &delayQueue{}

	heap.Push(dq, &task.Task{ID: "t2", ScheduleAt: now.Add(2 * time.Second)})
	heap.Push(dq, &task.Task{ID: "t1", ScheduleAt: now.Add(10 * time.Second), DispatchAhead: 9 * time.Second})

	if first := heap.Pop(dq).(*task.Task); first.ID != "t1" {
		t.Errorf("want t1 first (ScheduleTime=now+1s), got %s", first.ID)
	}
}

// ----------------------------------------------------------------
// dispatch()
//
// Called directly — no goroutines, fully deterministic.
// ----------------------------------------------------------------

func TestDispatch_DueTask_Enqueued(t *testing.T) {
	mq := &mockQueue{}
	s := makeScheduler(t, mq, &mockStore{})
	pushToDQ(s, makeTask("past-1", time.Now().Add(-time.Second)))

	s.dispatch(context.Background())

	if ids := mq.ids(); len(ids) != 1 || ids[0] != "past-1" {
		t.Errorf("want [past-1] enqueued, got %v", ids)
	}
}

func TestDispatch_FutureTask_NotEnqueued(t *testing.T) {
	mq := &mockQueue{}
	s := makeScheduler(t, mq, &mockStore{})
	pushToDQ(s, makeTask("future-1", time.Now().Add(time.Hour)))

	s.dispatch(context.Background())

	if ids := mq.ids(); len(ids) != 0 {
		t.Errorf("future task must not be enqueued, got %v", ids)
	}
}

func TestDispatch_Mixed_OnlyDueTasksEnqueued(t *testing.T) {
	mq := &mockQueue{}
	s := makeScheduler(t, mq, &mockStore{})
	pushToDQ(s,
		makeTask("past-1", time.Now().Add(-2*time.Second)),
		makeTask("past-2", time.Now().Add(-1*time.Second)),
		makeTask("future-1", time.Now().Add(time.Hour)),
	)

	s.dispatch(context.Background())

	ids := mq.ids()
	if len(ids) != 2 {
		t.Fatalf("want 2 enqueued, got %d: %v", len(ids), ids)
	}
	if ids[0] != "past-1" || ids[1] != "past-2" {
		t.Errorf("want [past-1 past-2] in heap order, got %v", ids)
	}
}

func TestDispatch_EmptyDQ_ReturnsDefaultNapTime(t *testing.T) {
	s := makeScheduler(t, &mockQueue{}, &mockStore{})

	if napTime := s.dispatch(context.Background()); napTime != defaultNapTime {
		t.Errorf("want defaultNapTime=%v, got %v", defaultNapTime, napTime)
	}
}

func TestDispatch_FutureTask_NapTimeApproximatesDelay(t *testing.T) {
	s := makeScheduler(t, &mockQueue{}, &mockStore{})
	delay := 500 * time.Millisecond
	pushToDQ(s, makeTask("future-1", time.Now().Add(delay)))

	napTime := s.dispatch(context.Background())

	if napTime > delay+50*time.Millisecond || napTime < 0 {
		t.Errorf("napTime should be ~%v, got %v", delay, napTime)
	}
}

func TestDispatch_EnqueueFails_ContinuesRemainingTasks(t *testing.T) {
	cq := &countingQueue{failOn: 1}
	s := makeScheduler(t, cq, &mockStore{})
	pushToDQ(s,
		makeTask("fail-task", time.Now().Add(-2*time.Second)),
		makeTask("ok-task", time.Now().Add(-1*time.Second)),
	)

	s.dispatch(context.Background())

	if len(cq.enqueuedIDs) != 1 || cq.enqueuedIDs[0] != "ok-task" {
		t.Errorf("want [ok-task] enqueued after first failure, got %v", cq.enqueuedIDs)
	}
}

// ----------------------------------------------------------------
// Submit()
// ----------------------------------------------------------------

func TestSubmit_ForcesStatusPending(t *testing.T) {
	ms := &mockStore{}
	s := makeScheduler(t, &mockQueue{}, ms)
	s.state.Store(StateRunning)

	tsk := &task.Task{
		ID:         "t1",
		ScheduleAt: time.Now().Add(time.Hour),
		Status:     task.StateSuccess,
	}
	if err := s.Submit(context.Background(), tsk); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	ms.mu.Lock()
	saved := ms.saved[0]
	ms.mu.Unlock()

	if saved.Status != task.StatePending {
		t.Errorf("want StatePending saved to store, got %v", saved.Status)
	}
}

func TestSubmit_MakesCopy_OuterMutationIgnored(t *testing.T) {
	ms := &mockStore{}
	s := makeScheduler(t, &mockQueue{}, ms)
	s.state.Store(StateRunning)

	originalTime := time.Now().Add(time.Hour)
	tsk := &task.Task{ID: "t-copy", ScheduleAt: originalTime}

	if err := s.Submit(context.Background(), tsk); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	tsk.ScheduleAt = time.Now().Add(99 * time.Hour)
	tsk.Name = "mutated"

	s.dqMu.Lock()
	inDQ := s.dq.items[0]
	s.dqMu.Unlock()

	if !inDQ.ScheduleAt.Equal(originalTime) {
		t.Errorf("dq item should keep original ScheduleAt, got %v", inDQ.ScheduleAt)
	}
	if inDQ.Name != "" {
		t.Errorf("dq item should not reflect outer mutation, got Name=%q", inDQ.Name)
	}
}

func TestSubmit_StoreSaveFails_TaskNotPushedToDQ(t *testing.T) {
	ms := &mockStore{saveErr: errors.New("db down")}
	s := makeScheduler(t, &mockQueue{}, ms)
	s.state.Store(StateRunning)

	tsk := &task.Task{ID: "t-store-fail", ScheduleAt: time.Now().Add(time.Hour)}
	if err := s.Submit(context.Background(), tsk); err == nil {
		t.Error("want error when store.Save fails, got nil")
	}

	s.dqMu.Lock()
	dqLen := s.dq.Len()
	s.dqMu.Unlock()

	if dqLen != 0 {
		t.Errorf("dq should be empty after store failure, got %d items", dqLen)
	}
}

func TestSubmit_WakeUp_SentWhenNewTaskIsEarliest(t *testing.T) {
	ms := &mockStore{}
	s := makeScheduler(t, &mockQueue{}, ms)
	s.state.Store(StateRunning)

	// Push a later task directly to avoid triggering wakeUp prematurely.
	pushToDQ(s, makeTask("later", time.Now().Add(time.Hour)))

	earlyTask := &task.Task{ID: "early", ScheduleAt: time.Now().Add(time.Minute)}
	if err := s.Submit(context.Background(), earlyTask); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	select {
	case <-s.wakeUp:
	default:
		t.Error("wakeUp signal should be sent when new task becomes the earliest")
	}
}

func TestSubmit_WakeUp_NotSentWhenNewTaskIsLater(t *testing.T) {
	ms := &mockStore{}
	s := makeScheduler(t, &mockQueue{}, ms)
	s.state.Store(StateRunning)

	pushToDQ(s, makeTask("early", time.Now().Add(time.Minute)))

	laterTask := &task.Task{ID: "later", ScheduleAt: time.Now().Add(time.Hour)}
	if err := s.Submit(context.Background(), laterTask); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	select {
	case <-s.wakeUp:
		t.Error("wakeUp signal must not be sent when new task is not the earliest")
	default:
	}
}

func TestSubmit_WakeUp_NoDuplicateSignal(t *testing.T) {
	ms := &mockStore{}
	s := makeScheduler(t, &mockQueue{}, ms)
	s.state.Store(StateRunning)

	s.wakeUp <- struct{}{} // fill the channel

	tsk := &task.Task{ID: "t-dup", ScheduleAt: time.Now().Add(time.Minute)}
	if err := s.Submit(context.Background(), tsk); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	if len(s.wakeUp) != 1 {
		t.Errorf("wakeUp should have exactly 1 signal, got %d", len(s.wakeUp))
	}
}

// ----------------------------------------------------------------
// Lifecycle
// ----------------------------------------------------------------

func TestNewDefaultScheduler_NilChecks(t *testing.T) {
	ms := &mockStore{}
	mq := &mockQueue{}

	if _, err := NewDefaultScheduler(nil, ms); err == nil {
		t.Error("nil queue should return error")
	}
	if _, err := NewDefaultScheduler(mq, nil); err == nil {
		t.Error("nil store should return error")
	}
}

func TestScheduler_CannotStartTwice(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := makeScheduler(t, &mockQueue{}, &mockStore{})
	if err := s.Start(ctx); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := s.Start(ctx); err == nil {
		t.Error("second Start should return error")
	}
}

func TestScheduler_SubmitWhenNotRunning_ReturnsError(t *testing.T) {
	s := makeScheduler(t, &mockQueue{}, &mockStore{})

	tsk := &task.Task{ID: "t1", ScheduleAt: time.Now()}
	if err := s.Submit(context.Background(), tsk); err == nil {
		t.Error("Submit when not Running should return error")
	}
}

func TestScheduler_Shutdown_Graceful(t *testing.T) {
	s := makeScheduler(t, &mockQueue{}, &mockStore{})
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := s.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

func TestScheduler_Shutdown_WhenNotRunning_ReturnsError(t *testing.T) {
	s := makeScheduler(t, &mockQueue{}, &mockStore{})
	if err := s.Shutdown(context.Background()); err == nil {
		t.Error("Shutdown when not Running should return error")
	}
}
