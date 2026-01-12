package queue

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"unsafe"
)

// ring implements a ring buffer as backing storage for Queue
type ring[T any] struct {
	buf        []T
	head, tail int
	size       int
	isPod      bool
	closed     atomic.Bool
	busy       atomic.Bool
}

// newRing creates a new ring buffer with the specified initial capacity.
func newRing[T any](capacity int) ring[T] {
	if capacity < 1 {
		capacity = 1
	}

	return ring[T]{
		buf:   make([]T, capacity),
		isPod: checkPOD[T](),
	}
}

func (r *ring[T]) String() string {
	return fmt.Sprintf("ring{cap=%d, len=%d, head=%d, tail=%d, closed=%v}", len(r.buf), r.size, r.head, r.tail, r.closed.Load())
}

// len returns the number of elements in the ring buffer.
func (r *ring[T]) len() int { return r.size }

// push adds an element to the ring buffer.
// If the ring is closed, the element is not added.
func (r *ring[T]) push(v T) {
	// Spin if busy
	for !r.busy.CompareAndSwap(false, true) {
	}
	if r.closed.Load() {
		r.busy.Store(false)
		return
	}
	if r.size == len(r.buf) {
		r.grow()
	}
	r.buf[r.tail] = v
	r.tail = (r.tail + 1) % len(r.buf)
	r.size++
	r.busy.Store(false)
}

// pop removes and returns the next element from the ring buffer.
// If the ring is closed, the zero value of T is returned.
func (r *ring[T]) pop() T {
	var zero T
	// Spin if busy
	for !r.busy.CompareAndSwap(false, true) {
	}
	if r.closed.Load() {
		r.busy.Store(false)
		return zero
	}
	v := r.buf[r.head]
	r.buf[r.head] = zero
	r.head = (r.head + 1) % len(r.buf)
	r.size--
	r.busy.Store(false)
	return v
}

// grow doubles the capacity of the ring buffer
func (r *ring[T]) grow() {

	newBuf := make([]T, nextslicecap(len(r.buf)))

	if r.isPod {
		r.copyUnsafe(newBuf)
	} else {
		r.copySafe(newBuf)
	}

	r.buf = newBuf
	r.head = 0
	r.tail = r.size
}

func (r *ring[T]) appendTo(other *ring[T]) {

	// Lock both rings
	for !other.busy.CompareAndSwap(false, true) {
	}

	for !r.busy.CompareAndSwap(false, true) {
	}

	if r.size == 0 || other.closed.Load() {
		other.busy.Store(false)
		r.busy.Store(false)
		return
	}

	// Ensure capacity in `other`
	required := other.size + r.size
	if required > len(other.buf) {
		newCap := len(other.buf)
		for newCap < required {
			newCap = nextslicecap(newCap)
		}

		newBuf := make([]T, newCap)
		if other.isPod {
			other.copyUnsafe(newBuf)
		} else {
			other.copySafe(newBuf)
		}

		other.buf = newBuf
		other.head = 0
		other.tail = other.size
	}

	// Append elements from r into other.buf starting at other.tail
	if r.isPod && other.isPod {
		other.appendUnsafeFrom(r)
	} else {
		other.appendSafeFrom(r)
	}

	other.size += r.size
	other.busy.Store(false)
	r.busy.Store(false)
}

// close marks the ring as closed and zeroes out the buffer to allow GC to reclaim memory
func (r *ring[T]) close() {

	r.closed.Store(true)

	// Spin if busy
	for !r.busy.CompareAndSwap(false, true) {
	}

	// Zero out the buffer so it can be reclaimed by GC
	r.buf = nil
	r.head = 0
	r.tail = 0
	r.size = 0

	r.busy.Store(false)
}

// copySafe is used to copy elements that are or contain pointers or slices
// so the GC does not lose track of them
func (r *ring[T]) copySafe(newBuf []T) {
	if r.head < r.tail {
		copy(newBuf, r.buf[r.head:r.tail])
	} else if r.size > 0 {
		n1 := len(r.buf) - r.head
		copy(newBuf[:n1], r.buf[r.head:])
		copy(newBuf[n1:], r.buf[:r.tail])
	}
}

// copyUnsafe more quickly copies pure scalar elements, arrays and structs containing scalars
// which occupy contiguous memory using block memory copy. The GC does not need
// to track the elements separately.
func (r *ring[T]) copyUnsafe(newBuf []T) {
	elemSize := unsafe.Sizeof(r.buf[0])
	if r.head < r.tail {
		n := r.tail - r.head
		memmove(unsafe.Pointer(&newBuf[0]), unsafe.Pointer(&r.buf[r.head]), uintptr(n)*elemSize)
	} else if r.size > 0 {
		n1 := len(r.buf) - r.head
		memmove(unsafe.Pointer(&newBuf[0]), unsafe.Pointer(&r.buf[r.head]), uintptr(n1)*elemSize)
		memmove(unsafe.Pointer(&newBuf[n1]), unsafe.Pointer(&r.buf[0]), uintptr(r.tail)*elemSize)
	}
}

func (other *ring[T]) appendSafeFrom(r *ring[T]) {
	dstCap := len(other.buf)
	dst := other.tail

	if r.head < r.tail {
		n := r.tail - r.head
		copy(other.buf[dst:], r.buf[r.head:r.tail])
		other.tail = (dst + n) % dstCap
	} else {
		if r.size == 0 {
			return
		}
		n1 := len(r.buf) - r.head
		copy(other.buf[dst:], r.buf[r.head:])
		dst = (dst + n1) % dstCap
		copy(other.buf[dst:], r.buf[:r.tail])
		other.tail = (dst + r.tail) % dstCap
	}
}

func (other *ring[T]) appendUnsafeFrom(r *ring[T]) {
	elemSize := unsafe.Sizeof(r.buf[0])
	dstCap := len(other.buf)
	dst := other.tail

	if r.head < r.tail {
		n := r.tail - r.head
		memmove(
			unsafe.Pointer(&other.buf[dst]),
			unsafe.Pointer(&r.buf[r.head]),
			uintptr(n)*elemSize,
		)
		other.tail = (dst + n) % dstCap
	} else {
		if r.size == 0 {
			return
		}
		n1 := len(r.buf) - r.head
		memmove(
			unsafe.Pointer(&other.buf[dst]),
			unsafe.Pointer(&r.buf[r.head]),
			uintptr(n1)*elemSize,
		)
		dst = (dst + n1) % dstCap
		memmove(
			unsafe.Pointer(&other.buf[dst]),
			unsafe.Pointer(&r.buf[0]),
			uintptr(r.tail)*elemSize,
		)
		other.tail = (dst + r.tail) % dstCap
	}
}

// checkPOD checks whether the element type is purely scalar (Plain Old Datatype)
func checkPOD[T any]() bool {
	var t T
	rt := reflect.TypeOf(t)

	switch rt.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Float32, reflect.Float64, reflect.Bool, reflect.Complex64, reflect.Complex128:
		// scalar type
		return true
	case reflect.Array, reflect.Struct:
		// Recursivley check compount element type
		for i := 0; i < rt.NumField(); i++ {
			if !isPODType(rt.Field(i).Type) {
				// struct or array contains at least obe non-scalar type
				return false
			}
		}
		// struct or array containing only scalar types
		return true
	default:
		// Non-scalar (including string, slice, pointer etc.)
		return false
	}
}

func isPODType(rt reflect.Type) bool {
	switch rt.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Float32, reflect.Float64, reflect.Bool, reflect.Complex64, reflect.Complex128:
		return true
	case reflect.Array, reflect.Struct:
		for i := 0; i < rt.NumField(); i++ {
			if !isPODType(rt.Field(i).Type) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// nextslicecap computes the next appropriate slice length.
// This is adapted from the built-in Go slice growth algorithm.
func nextslicecap(oldCap int) int {
	newLen := oldCap + 1
	newcap := oldCap
	doublecap := newcap + newcap
	if newLen > doublecap {
		return newLen
	}
	const threshold = 1024
	if oldCap < threshold {
		return doublecap
	}
	for {
		// Transition from growing 2x for small slices
		// to growing 1.25x for large slices. This formula
		// gives a smooth-ish transition between the two.
		newcap += (newcap + 3*threshold) >> 2
		// We need to check `newcap >= newLen` and whether `newcap` overflowed.
		// newLen is guaranteed to be larger than zero, hence
		// when newcap overflows then `uint(newcap) > uint(newLen)`.
		// This allows to check for both with the same comparison.
		if uint(newcap) >= uint(newLen) {
			break
		}
	}
	// Set newcap to the requested cap when
	// the newcap calculation overflowed.
	if newcap <= 0 {
		return newLen
	}
	return newcap
}

// memmove wraps runtime.memmove
//
//go:linkname memmove runtime.memmove
func memmove(dst, src unsafe.Pointer, n uintptr)
