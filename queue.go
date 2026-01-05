// Package queue implements a lock-free channel-based queue.
//
// It is not designed for raw speed, but rather to be used as a data source
// for things like worker pools where reading input from a channel makes sense.
package queue

import (
	"errors"
	"sync/atomic"
)

type Queue[T any] struct {
	enqCh   chan T
	deqCh   chan T
	closeCh chan struct{}
	closed  atomic.Bool

	// internal ring buffer
	rb ring[T]
}

type qopts struct {
	initialCapacity int
}

type QueueOptFunc func(*qopts)

// ErrQueueClosed is returned by Enqueue if the Close method has been called.
var ErrQueueClosed = errors.New("queue is closed")

// New creates a new queue.
func New[T any](opts ...QueueOptFunc) *Queue[T] {

	options := qopts{
		initialCapacity: 256,
	}

	for _, o := range opts {
		o(&options)
	}

	q := &Queue[T]{
		enqCh:   make(chan T),
		deqCh:   make(chan T),
		closeCh: make(chan struct{}),
		rb:      newRing[T](options.initialCapacity),
	}

	go q.run()
	return q
}

// WithInitialCapacity is a constructor function to set the initial
// capacity of the queue's internal ring buffer.
func WithInitialCapacity(capacity int) QueueOptFunc {
	return func(o *qopts) {
		o.initialCapacity = capacity
	}
}

// Enqueue adds an element to the back of the queue.
// If the queue is closed, an error is returned.
//
// You must call Close() when finished appending elements
// to prevent the Dequeue channel from blocking when the queue empties.
func (q *Queue[T]) Enqueue(v T) error {
	select {
	case <-q.closeCh:
		return ErrQueueClosed
	case q.enqCh <- v:
		return nil
	}
}

// Dequeue returns the queue's read channel. Reading the channel will block if the queue is empty,
// or return false on v,ok if the queue has been closed.
func (q *Queue[T]) Dequeue() <-chan T {
	return q.deqCh
}

// Len returns the queue length, i.e. number of items in the queue.
func (q *Queue[T]) Len() int {
	return q.rb.len()
}

// Close closes the queue's channels. Items can be dequeued until the queue is empty
// at which point reading the dequeue channel with v,ok returns false
func (q *Queue[T]) Close() {
	select {
	case <-q.closeCh:
	default:
		close(q.closeCh)
		q.closed.Store(true)
	}
}

// Drain empties the queue of any reaining elements.
//
// You should call this if you do not expect your consumers
// to empty the queue themselves, else a goroutine and some
// memory will be leaked.
func (q *Queue[T]) Drain() {
	if !q.closed.Load() {
		return
	}

	q.rb.close()
}

// run is a goroutine that services the queue's channels.
// It exits when queue is closed and the backing store is empty.
func (q *Queue[T]) run() {
	defer close(q.deqCh)

	closed := false

	for {
		var (
			outCh chan T
			outV  T
		)

		if q.rb.len() > 0 {
			outCh = q.deqCh
			outV = q.rb.buf[q.rb.head]
		}

		select {
		case v := <-q.enqCh:
			q.rb.push(v)
		case outCh <- outV:
			q.rb.pop()
		case <-q.closeCh:
			closed = true
		}

		if closed && q.rb.len() == 0 {
			return
		}
	}
}
