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

type Future[T any] func(t T)

type pool[T any] struct {
	core     int
	max      int
	ch       chan T
	capacity int
	reject   strategy
	goTTL    time.Duration

	stop     atomic.Bool
	cur      atomic.Int64
	f        Future[T]
	mu       sync.Mutex
	nodeHead *node[T]
}

func NewPool[T any](core, max, capacity int, ttl time.Duration, reject strategy, f Future[T]) (*pool[T], error) {
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
		f:        f,
		goTTL:    ttl,
	}
	// build goroutines

	head := p.newNode(nil)
	p.nodeHead = head
	for i := 0; i < p.core-1; i++ {
		tempNode := p.newNode(nil)
		p.addNode(tempNode)
	}

	return p, nil
}

func (p *pool[T]) Submit(t T) error {
	if p.stop.Load() {
		return ErrPoolStopped
	} else if p.reject == Block {
		p.ch <- t
		return nil
	} else if p.isFull() {
		if p.reject == Reject {
			return ErrRejectByPoolIsFull
		} else {
			p.f(t)
		}
	}
	return nil
}

func (p *pool[T]) Stop() []T {
	p.stop.Store(true)
	nh := p.nodeHead
	nodes := make([]*node[T], 0, p.max)
	for nh != nil {
		nodes = append(nodes, nh)
		nh = nh.next
	}
	for n := range nodes {
		nodes[n].isFinish.Store(true)
	}
	for p.nodeHead != nil {
		// wait for all nodes to finish
	}
	result := make([]T, 0, p.capacity)
	for len(p.ch) != 0 {
		result = append(result, <-p.ch)
	}
	return result
}

func (p *pool[T]) monitor() {
	if cur := p.cur.Load(); len(p.ch) == p.capacity && cur <= int64(p.max) {
		node := p.newNode(&p.goTTL)
		p.addNode(node)
	}
}

func (p *pool[T]) isFull() bool {
	return p.cur.Load() >= int64(p.max) && len(p.ch) >= p.capacity
}

func (p *pool[T]) addNode(n *node[T]) {
	if n == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cur.Load() >= int64(p.max) {
		return
	} else {
		p.cur.Add(1)
	}

	if p.nodeHead == nil {
		p.nodeHead = n
	} else {
		n.next = p.nodeHead
		p.nodeHead.prev = n
		p.nodeHead = n
	}
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

	p.mu.Lock()
	defer p.mu.Unlock()
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
}

func (n *node[T]) run() {
	go func() {
		for {
			select {
			case t := <-n.p.ch:
				n.startAt = time.Now()
				n.p.f(t)
				n.p.monitor()
			default:
				if n.isFinish.Load() {
					n.remove()
					return
				} else if n.duration != nil && time.Since(n.startAt) > *n.duration {
					n.isFinish.Store(true)
				}
			}
		}
	}()
}
