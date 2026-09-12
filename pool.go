package pool

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

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

type Runnable[T any] func(t T)

type pool[T any] struct {
	core     int
	max      int
	ch       chan T
	capacity int
	reject   strategy
	ttl      time.Duration
	run      Runnable[T]
	// inside control
	stop     atomic.Bool
	cur      atomic.Int64
	mu       sync.Mutex
	nodeHead *node[T]
	done     chan struct{}
	wg       sync.WaitGroup
}

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

	for i := 0; i < p.core; i++ {
		p.newNode(nil)
	}

	return p, nil
}

func (p *pool[T]) Submit(t T) error {
	if p.stop.Load() {
		return ErrPoolStopped
	}

	if p.reject == Block {
		p.ch <- t
		return nil
	}

	if p.isFull() {
		if p.reject == Reject {
			return ErrRejectByPoolIsFull
		}
		p.run(t)
		return nil
	}

	p.ch <- t
	return nil
}

func (p *pool[T]) Stop() []T {
	p.stop.Store(true)
	close(p.done)
	p.wg.Wait()

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

func (p *pool[T]) monitor() {
	if p.cur.Load() < int64(p.max) && len(p.ch) == p.capacity {
		p.newNode(&p.ttl)
	}
}

func (p *pool[T]) isFull() bool {
	return p.cur.Load() >= int64(p.max) && len(p.ch) >= p.capacity
}

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

	if p.nodeHead == nil {
		p.nodeHead = n
	} else {
		n.next = p.nodeHead
		p.nodeHead.prev = n
		p.nodeHead = n
	}
	return true
}

type node[T any] struct {
	p *pool[T]

	isFinish   atomic.Bool
	startAt    time.Time
	duration   *time.Duration
	prev, next *node[T]
}

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

func (n *node[T]) run() {
	go func() {
		for {
			select {
			case <-n.p.done:
				n.remove()
				return
			case t := <-n.p.ch:
				n.startAt = time.Now()
				n.p.run(t)
				n.p.monitor()
			default:
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
