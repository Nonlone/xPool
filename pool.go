package pool

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// strategy defines the overflow behavior when the pool is full.
type strategy int

const (
	_         strategy = iota
	Reject             // reject new tasks when pool is full
	CallerRun          // run in caller
	Block              // block for chan
)

var (
	ErrInvalidParams      = errors.New("invalid params")
	ErrCoreGreaterThanMax = errors.New("core greater than max")
	ErrRejectByPoolIsFull = errors.New("reject by pool is full")
	ErrPoolStopped        = errors.New("pool stopped")
)

// Runnable is a task handler function type.
type Runnable[T any] func(t T)

// pool is a generic goroutine pool with core-max scaling model.
// Core goroutines are always alive; extra goroutines are created on demand
// and expire after TTL when idle.
type pool[T any] struct {
	core     int           // number of core goroutines (always alive)
	max      int           // maximum goroutine count
	ch       chan T        // buffered task channel
	capacity int           // channel buffer size
	reject   strategy      // overflow strategy
	ttl      time.Duration // idle timeout for extra goroutines
	run      Runnable[T]   // task handler

	// internal control fields
	stop     atomic.Bool   // closed flag, set by Stop()
	cur      atomic.Int64  // current number of active goroutines
	mu       sync.Mutex    // protects linked list operations
	nodeHead *node[T]      // head of the doubly-linked list of goroutines
	done     chan struct{}  // closed by Stop() to signal all goroutines to exit
	wg       sync.WaitGroup // tracks active goroutines for graceful shutdown
}

// NewRunnable creates a new goroutine pool.
// core: core goroutine count (always alive).
// max: maximum goroutine count.
// capacity: task channel buffer size.
// ttl: idle timeout for extra goroutines.
// reject: overflow strategy when pool is full.
// run: task handler function.
func NewRunnable[T any](core, max, capacity int, ttl time.Duration, reject strategy, run Runnable[T]) (*pool[T], error) {
	if core <= 0 || max <= 0 || capacity <= 0 {
		return nil, ErrInvalidParams
	} else if core > max {
		return nil, ErrCoreGreaterThanMax
	}

	p := &pool[T]{
		core:     core,
		max:      max,
		ch:       make(chan T, capacity),
		capacity: capacity,
		reject:   reject,
		run:      run,
		ttl:      ttl,
		done:     make(chan struct{}),
	}

	// Create core goroutines (no TTL, never expire)
	for i := 0; i < p.core; i++ {
		p.newNode(nil)
	}

	return p, nil
}

// Submit sends a task to the pool.
// Returns error if pool is full (Reject strategy) or pool is stopped.
// Block strategy: blocks until a slot is available.
// CallerRun strategy: runs the task in the caller goroutine when full.
func (p *pool[T]) Submit(t T) error {
	if p.stop.Load() {
		return ErrPoolStopped
	}

	// Block strategy: always send to channel (blocks if full)
	if p.reject == Block {
		p.ch <- t
		return nil
	}

	if p.isFull() {
		if p.reject == Reject {
			return ErrRejectByPoolIsFull
		}
		// CallerRun: execute in the caller goroutine
		p.run(t)
		return nil
	}

	p.ch <- t
	return nil
}

// Stop closes the pool, waits for all goroutines to finish,
// and returns unprocessed tasks remaining in the channel.
func (p *pool[T]) Stop() []T {
	p.stop.Store(true)
	close(p.done)
	p.wg.Wait()

	// Drain remaining tasks from channel
	result := make([]T, 0)
	for {
		select {
		case t := <-p.ch:
			result = append(result, t)
		default:
			return result
		}
	}
}

// monitor checks if a new goroutine should be created.
// Creates an extra goroutine with TTL when channel is full and cur < max.
func (p *pool[T]) monitor() {
	if p.cur.Load() < int64(p.max) && len(p.ch) == p.capacity {
		p.newNode(&p.ttl)
	}
}

// isFull returns true when goroutine count >= max AND channel is full.
func (p *pool[T]) isFull() bool {
	return p.cur.Load() >= int64(p.max) && len(p.ch) >= p.capacity
}

// addNode adds a node to the head of the doubly-linked list.
// Returns false if node is nil or pool has reached max goroutines.
func (p *pool[T]) addNode(n *node[T]) bool {
	if n == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cur.Load() >= int64(p.max) {
		return false
	}
	p.cur.Add(1)

	// Insert at head
	if p.nodeHead == nil {
		p.nodeHead = n
	} else {
		n.next = p.nodeHead
		p.nodeHead.prev = n
		p.nodeHead = n
	}
	return true
}

// node represents a single goroutine worker in the pool,
// managed as a node in a doubly-linked list.
type node[T any] struct {
	p *pool[T] // reference to parent pool

	isFinish   atomic.Bool    // set to true when TTL expires
	startAt    time.Time      // when this goroutine started or last processed a task
	duration   *time.Duration // TTL duration (nil for core goroutines)
	prev, next *node[T]       // doubly-linked list pointers
}

// newNode creates a new goroutine worker node.
// d is the TTL duration (nil for core goroutines that never expire).
func (p *pool[T]) newNode(d *time.Duration) *node[T] {
	n := &node[T]{
		p:        p,
		startAt:  time.Now(),
		duration: d,
	}

	if !p.addNode(n) {
		return nil
	}

	p.wg.Add(1)
	n.run()
	return n
}

// remove unlinks this node from the doubly-linked list and decrements the counter.
func (n *node[T]) remove() {
	n.p.mu.Lock()
	defer n.p.mu.Unlock()

	if n.prev != nil {
		n.prev.next = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	}
	if n.p.nodeHead == n {
		n.p.nodeHead = n.next
	}
	n.p.cur.Add(-1)
	n.p.wg.Done()
}

// run starts the goroutine worker loop.
// It selects between: receiving done signal, processing tasks, or checking TTL expiry.
func (n *node[T]) run() {
	go func() {
		for {
			select {
			case <-n.p.done:
				// Pool stopped: unlink and exit
				n.remove()
				return
			case t := <-n.p.ch:
				// Process task, then check if we should scale up
				n.startAt = time.Now()
				n.p.run(t)
				n.p.monitor()
			default:
				// No task available: check TTL expiry for extra goroutines
				if n.isFinish.Load() {
					n.remove()
					return
				}
				if n.duration != nil && time.Since(n.startAt) > *n.duration {
					n.isFinish.Store(true)
				}
			}
		}
	}()
}

// -----------------------------------------------------------------------
// Callable: wraps a function that returns a result into the pool.
// -----------------------------------------------------------------------

// Callable is a task function that returns a result of type K.
type Callable[T, K any] func(t T) K

// doCall is an internal wrapper that pairs a task with its result channel.
type doCall[T, K any] struct {
	t  T
	ch chan K
}

// call is a typed wrapper around pool[doCall[T, K]] for Callable usage.
type call[T, K any] struct {
	p *pool[doCall[T, K]]
}

// NewCallable creates a Callable pool that wraps a result-returning function.
// Submit returns a channel that will receive the result when the task completes.
func NewCallable[T, K any](core, max, capacity int, ttl time.Duration, reject strategy, callable Callable[T, K]) (*call[T, K], error) {

	run := func(e doCall[T, K]) {
		k := callable(e.t)
		e.ch <- k
		close(e.ch)
	}

	p, err := NewRunnable[doCall[T, K]](core, max, capacity, ttl, reject, run)
	if err != nil {
		return nil, err
	}
	return &call[T, K]{
		p: p,
	}, nil
}

// Submit sends a task and returns a channel that will receive the result.
// Returns error if pool is full (Reject) or stopped.
func (c *call[T, K]) Submit(t T) (chan K, error) {
	d := doCall[T, K]{
		t:  t,
		ch: make(chan K, 1),
	}
	if err := c.p.Submit(d); err != nil {
		return nil, err
	}
	return d.ch, nil
}

// Stop stops the pool and returns unprocessed original tasks.
func (c *call[T, K]) Stop() []T {
	ds := c.p.Stop()
	result := make([]T, 0, len(ds))
	for _, d := range ds {
		result = append(result, d.t)
	}
	return result
}

// Result collects results from multiple result channels.
func (c *call[T, K]) Result(ch []chan K) []K {
	result := make([]K, 0, len(ch))
	for _, ch := range ch {
		for k := range ch {
			result = append(result, k)
		}
	}
	return result
}
