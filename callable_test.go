package pool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCallable_BasicExecution(t *testing.T) {
	f := func(i int) int { return i * 2 }

	p, err := NewCallable(2, 4, 10, time.Second, Reject, f)
	if err != nil {
		t.Fatal(err)
	}

	ch, err := p.Submit(3)
	if err != nil {
		t.Fatal(err)
	}

	result := <-ch
	if result != 6 {
		t.Errorf("result = %d, want 6", result)
	}
}

func TestCallable_MultipleSubmits(t *testing.T) {
	f := func(i int) string { return "done" }

	p, err := NewCallable(2, 4, 10, time.Second, Reject, f)
	if err != nil {
		t.Fatal(err)
	}

	var channels []chan string
	for i := 0; i < 5; i++ {
		ch, err := p.Submit(i)
		if err != nil {
			t.Fatal(err)
		}
		channels = append(channels, ch)
	}

	results := p.Result(channels)
	if len(results) != 5 {
		t.Errorf("results len = %d, want 5", len(results))
	}
	for _, r := range results {
		if r != "done" {
			t.Errorf("result = %q, want %q", r, "done")
		}
	}
}

func TestCallable_RejectStrategy(t *testing.T) {
	started := make(chan struct{})
	block := make(chan struct{})
	f := func(i int) int {
		if i == 0 {
			close(started)
			<-block
		}
		return i
	}

	p, err := NewCallable(1, 1, 1, time.Second, Reject, f)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { close(block); p.Stop() }()

	// First submit succeeds, goroutine blocks
	_, err = p.Submit(0)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	time.Sleep(10 * time.Millisecond)

	// Channel fills up, next submit fills it, third should be rejected
	_, err = p.Submit(1)
	if err != nil {
		t.Fatal(err)
	}

	// Pool is full (cur=1, ch full) — should reject
	ch, err := p.Submit(2)
	if err != ErrRejectByPoolIsFull {
		t.Errorf("Submit(2) error = %v, want %v", err, ErrRejectByPoolIsFull)
	}
	if ch != nil {
		t.Error("rejected Submit should return nil channel")
	}
}

func TestCallable_CallerRunStrategy(t *testing.T) {
	started := make(chan struct{})
	block := make(chan struct{})
	f := func(i int) int {
		if i == 0 {
			close(started)
			<-block
		}
		return i + 100
	}

	p, err := NewCallable(1, 1, 1, time.Second, CallerRun, f)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { close(block); p.Stop() }()

	// Submit task 0, goroutine blocks
	_, err = p.Submit(0)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	time.Sleep(10 * time.Millisecond)

	// Channel capacity=1, goroutine busy processing task 0
	// Fill the channel manually via inner pool
	p.p.ch <- doCall[int, int]{t: 99, ch: make(chan int, 1)}

	// Now isFull() = true (cur=1, ch full) → CallerRun executes in caller
	ch2, err := p.Submit(1)
	if err != nil {
		t.Errorf("CallerRun Submit error = %v", err)
	}
	if ch2 == nil {
		t.Error("CallerRun should return a channel")
	}

	// CallerRun runs the callable in the caller goroutine, result is sent to channel
	result := <-ch2
	if result != 101 {
		t.Errorf("CallerRun result = %d, want 101", result)
	}
}

func TestCallable_BlockStrategy(t *testing.T) {
	f := func(i int) int { return i * 10 }

	p, err := NewCallable(1, 1, 1, time.Second, Block, f)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	channels := make([]chan int, 3)
	done := make(chan error, 3)
	for i := 1; i <= 3; i++ {
		go func(idx, v int) {
			ch, err := p.Submit(v)
			if err != nil {
				done <- err
				return
			}
			channels[idx] = ch
			done <- nil
		}(i-1, i)
	}

	for i := 0; i < 3; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Submit error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Block Submit timed out")
		}
	}

	for i, ch := range channels {
		if ch == nil {
			continue
		}
		result := <-ch
		expected := (i + 1) * 10
		if result != expected {
			t.Errorf("result[%d] = %d, want %d", i, result, expected)
		}
	}
}

func TestCallable_AfterStop(t *testing.T) {
	f := func(i int) int { return i }

	p, err := NewCallable(2, 4, 10, time.Second, Reject, f)
	if err != nil {
		t.Fatal(err)
	}
	p.Stop()

	ch, err := p.Submit(1)
	if err != ErrPoolStopped {
		t.Errorf("Submit after Stop error = %v, want %v", err, ErrPoolStopped)
	}
	if ch != nil {
		t.Error("Submit after Stop should return nil channel")
	}
}

func TestCallable_StopReturnsRemainingTasks(t *testing.T) {
	block := make(chan struct{})
	f := func(i int) int {
		if i == 0 {
			<-block
		}
		return i
	}

	p, err := NewCallable(1, 1, 10, time.Second, Reject, f)
	if err != nil {
		t.Fatal(err)
	}

	p.Submit(0)
	time.Sleep(10 * time.Millisecond)

	// Submit more tasks that queue up
	for i := 1; i <= 5; i++ {
		p.Submit(i)
	}

	close(block)
	remaining := p.Stop()

	if len(remaining) > 5 {
		t.Errorf("remaining tasks = %d, want <= 5", len(remaining))
	}
}

func TestCallable_Concurrent(t *testing.T) {
	var count atomic.Int64
	f := func(i int) int {
		count.Add(1)
		return i
	}

	p, err := NewCallable(4, 8, 1000, time.Second, Block, f)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			ch, err := p.Submit(v)
			if err != nil {
				t.Errorf("Submit(%d) error = %v", v, err)
				return
			}
			result := <-ch
			if result != v {
				t.Errorf("result = %d, want %d", result, v)
			}
		}(i)
	}
	wg.Wait()

	if count.Load() != 100 {
		t.Errorf("executed tasks = %d, want 100", count.Load())
	}

	p.Stop()
}

func TestCallable_InvalidParams(t *testing.T) {
	f := func(i int) int { return i }

	_, err := NewCallable(0, 10, 10, time.Second, Reject, f)
	if err != ErrInvalidParams {
		t.Errorf("NewCallable(0,...) error = %v, want %v", err, ErrInvalidParams)
	}

	_, err = NewCallable(11, 10, 10, time.Second, Reject, f)
	if err != ErrCoreGreaterThanMax {
		t.Errorf("NewCallable(core>max) error = %v, want %v", err, ErrCoreGreaterThanMax)
	}
}

func TestCallable_ResultEmpty(t *testing.T) {
	f := func(i int) int { return i }

	p, err := NewCallable(2, 4, 10, time.Second, Reject, f)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	results := p.Result(nil)
	if len(results) != 0 {
		t.Errorf("Result(nil) len = %d, want 0", len(results))
	}

	results = p.Result([]chan int{})
	if len(results) != 0 {
		t.Errorf("Result([]) len = %d, want 0", len(results))
	}
}
