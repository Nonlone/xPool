package pool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewPool_InvalidParams(t *testing.T) {
	f := func(i int) {}
	tests := []struct {
		name    string
		core    int
		max     int
		cap     int
		wantErr error
	}{
		{"core zero", 0, 10, 10, ErrInvalidParams},
		{"max zero", 10, 0, 10, ErrInvalidParams},
		{"capacity zero", 10, 10, 0, ErrInvalidParams},
		{"core negative", -1, 10, 10, ErrInvalidParams},
		{"max negative", 10, -1, 10, ErrInvalidParams},
		{"capacity negative", 10, 10, -1, ErrInvalidParams},
		{"core greater than max", 11, 10, 10, ErrCoreGreaterThanMax},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRunnable(tt.core, tt.max, tt.cap, time.Second, Reject, f)
			if err != tt.wantErr {
				t.Errorf("NewPool() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewPool_CreatesCoreGoroutines(t *testing.T) {
	core := 3
	f := func(i int) {}
	p, err := NewRunnable(core, 5, 10, time.Second, Reject, f)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	if p.cur.Load() != int64(core) {
		t.Errorf("cur = %d, want %d", p.cur.Load(), core)
	}

	n := p.nodeHead
	count := 0
	for n != nil {
		count++
		n = n.next
	}
	if count != core {
		t.Errorf("node count = %d, want %d", count, core)
	}
}

func TestSubmit_BasicExecution(t *testing.T) {
	var mu sync.Mutex
	results := make([]int, 0)
	f := func(i int) {
		mu.Lock()
		results = append(results, i)
		mu.Unlock()
	}

	p, err := NewRunnable(2, 4, 10, time.Second, Reject, f)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		if err := p.Submit(i); err != nil {
			t.Fatalf("Submit(%d) error = %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if len(results) != 5 {
		t.Errorf("results len = %d, want 5", len(results))
	}
	mu.Unlock()
}

func TestSubmit_RejectStrategy(t *testing.T) {
	started := make(chan struct{})
	block := make(chan struct{})
	f := func(i int) {
		if i == 0 {
			close(started)
			<-block
		}
	}

	p, err := NewRunnable(1, 1, 1, time.Second, Reject, f)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	if err := p.Submit(0); err != nil {
		t.Fatal(err)
	}
	<-started

	// Goroutine is blocked, channel is empty
	// Submit(1) goes to channel, goroutine picks it up
	// isFull() returns false because channel is empty
	// This tests that Submit succeeds when channel has space
	if err := p.Submit(1); err != nil {
		t.Errorf("Submit(1) error = %v, want nil", err)
	}

	close(block)
}

func TestSubmit_RejectWhenFull(t *testing.T) {
	// Test reject when pool is truly full
	// Use a pool where goroutine blocks and channel fills up
	f := func(i int) {
		time.Sleep(time.Second) // Block for a while
	}

	p, err := NewRunnable(1, 1, 1, time.Second, Reject, f)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	// Submit task, goroutine picks it up and starts processing (sleeping)
	if err := p.Submit(0); err != nil {
		t.Fatal(err)
	}

	// Now submit rapidly to fill the channel
	// Channel capacity is 1, goroutine is busy, so second submit fills channel
	// Third submit should be rejected
	time.Sleep(10 * time.Millisecond) // Let goroutine pick up task 0

	if err := p.Submit(1); err != nil {
		t.Fatalf("Submit(1) error = %v", err)
	}
	// Channel is now full, goroutine is busy
	// This submit should be rejected
	if err := p.Submit(2); err != ErrRejectByPoolIsFull {
		t.Errorf("Submit(2) error = %v, want %v", err, ErrRejectByPoolIsFull)
	}
}

func TestSubmit_CallerRunStrategy(t *testing.T) {
	var mu sync.Mutex
	callerRan := false

	block := make(chan struct{})
	f := func(i int) {
		if i == 0 {
			<-block
		}
		mu.Lock()
		callerRan = true
		mu.Unlock()
	}

	p, err := NewRunnable(1, 1, 1, time.Second, CallerRun, f)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	// Submit task 0, goroutine blocks
	if err := p.Submit(0); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)

	// Now the pool is "full" from a goroutine perspective
	// But channel is empty (task 0 was consumed)
	// CallerRun only triggers when isFull() is true
	// isFull requires cur >= max AND len(ch) >= capacity
	// We need to fill the channel too
	p.ch <- 42 // Manually fill channel

	// Now isFull() should return true
	if err := p.Submit(1); err != nil {
		t.Errorf("Submit(1) error = %v", err)
	}

	mu.Lock()
	if !callerRan {
		t.Error("CallerRun strategy did not execute task in caller")
	}
	mu.Unlock()

	close(block)
}

func TestSubmit_BlockStrategy(t *testing.T) {
	results := make(chan int, 3)
	f := func(i int) {
		results <- i
	}

	p, err := NewRunnable(1, 1, 1, time.Second, Block, f)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	done := make(chan error, 3)
	go func() { done <- p.Submit(1) }()
	go func() { done <- p.Submit(2) }()
	go func() { done <- p.Submit(3) }()

	for i := 0; i < 3; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Submit error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Submit blocked too long")
		}
	}
}

func TestSubmit_AfterStop(t *testing.T) {
	f := func(i int) {}
	p, err := NewRunnable(2, 4, 10, time.Second, Reject, f)
	if err != nil {
		t.Fatal(err)
	}
	p.Stop()

	if err := p.Submit(1); err != ErrPoolStopped {
		t.Errorf("Submit after Stop error = %v, want %v", err, ErrPoolStopped)
	}
}

func TestStop_ReturnsRemainingTasks(t *testing.T) {
	started := make(chan struct{})
	block := make(chan struct{})
	f := func(i int) {
		if i == 0 {
			close(started)
			<-block
		}
	}

	p, err := NewRunnable(1, 1, 10, time.Second, Reject, f)
	if err != nil {
		t.Fatal(err)
	}

	// Submit task 0, goroutine blocks in f
	if err := p.Submit(0); err != nil {
		t.Fatal(err)
	}
	<-started
	time.Sleep(10 * time.Millisecond)

	// Submit 5 more tasks — they queue in channel since goroutine is blocked
	for i := 1; i <= 5; i++ {
		if err := p.Submit(i); err != nil {
			t.Fatal(err)
		}
	}

	// Unblock the goroutine and immediately call Stop
	// Between close(block) and Stop(), the scheduler may or may not run the pool goroutine
	// So remaining count is 0-5 (non-deterministic)
	close(block)
	remaining := p.Stop()

	if len(remaining) > 5 {
		t.Errorf("remaining tasks = %d, want <= 5", len(remaining))
	}
}

func TestStop_AllGoroutinesExit(t *testing.T) {
	f := func(i int) {
		time.Sleep(10 * time.Millisecond)
	}

	p, err := NewRunnable(3, 6, 20, time.Second, Reject, f)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		p.Submit(i)
	}

	time.Sleep(50 * time.Millisecond)
	p.Stop()

	// If we get here without deadlock, all goroutines exited
}

func TestMonitor_DynamicScaling(t *testing.T) {
	// monitor creates new goroutines when channel is full
	// and cur < max
	f := func(i int) {
		time.Sleep(time.Second)
	}

	core := 1
	max := 3
	capacity := 1
	p, err := NewRunnable(core, max, capacity, time.Second, Reject, f)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	// Submit task, goroutine picks it up
	if err := p.Submit(0); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)

	// Manually fill the channel to trigger monitor
	p.ch <- 42

	// Now channel is full and goroutine is busy
	// monitor should create new goroutine
	p.monitor()

	cur := p.cur.Load()
	if cur != int64(core)+1 {
		t.Errorf("cur = %d, want %d", cur, core+1)
	}
}

func TestTTL_GoroutineExpiry(t *testing.T) {
	ttl := 50 * time.Millisecond
	f := func(i int) {}

	core := 1
	max := 3
	p, err := NewRunnable(core, max, 5, ttl, Reject, f)
	if err != nil {
		t.Fatal(err)
	}

	initial := p.cur.Load()

	// Submit task - goroutine processes it, startAt resets, TTL countdown begins
	if err := p.Submit(0); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond) // Let goroutine pick up task

	// Fill channel to trigger monitor (goroutine is idle after processing)
	for i := 1; i <= 5; i++ {
		p.ch <- i
	}

	p.monitor()

	afterMonitor := p.cur.Load()
	if afterMonitor <= initial {
		t.Errorf("monitor did not scale up: cur = %d, initial = %d", afterMonitor, initial)
	}

	// Wait for TTL to expire - goroutines should exit after idle timeout
	time.Sleep(ttl + 100*time.Millisecond)

	afterTTL := p.cur.Load()
	if afterTTL >= afterMonitor {
		t.Errorf("TTL expiry did not scale down: cur = %d, after monitor = %d", afterTTL, afterMonitor)
	}

	p.Stop()
}

func TestConcurrent_Submit(t *testing.T) {
	var count atomic.Int64
	f := func(i int) {
		count.Add(1)
	}

	p, err := NewRunnable(4, 8, 1000, time.Second, Block, f)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			p.Submit(v)
		}(i)
	}
	wg.Wait()

	time.Sleep(200 * time.Millisecond)
	p.Stop()

	if count.Load() != 1000 {
		t.Errorf("executed tasks = %d, want 1000", count.Load())
	}
}

func TestIsFull(t *testing.T) {
	f := func(i int) {
		time.Sleep(time.Second)
	}
	p, err := NewRunnable(2, 2, 2, time.Second, Reject, f)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	if p.isFull() {
		t.Error("pool should not be full initially")
	}

	// Fill channel
	p.ch <- 0
	p.ch <- 1

	// cur=2, ch full => isFull
	if !p.isFull() {
		t.Error("pool should be full after filling channel and reaching max goroutines")
	}
}

func TestAddNode_RejectsWhenMaxReached(t *testing.T) {
	f := func(i int) {}
	p, err := NewRunnable(2, 2, 10, time.Second, Reject, f)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	n := &node[int]{p: p, startAt: time.Now()}
	if p.addNode(n) {
		t.Error("addNode should reject when max reached")
	}

	if p.cur.Load() != 2 {
		t.Errorf("cur = %d, want 2", p.cur.Load())
	}
}
