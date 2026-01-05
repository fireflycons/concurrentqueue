package queue

import (
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
	popping    atomic.Bool
}

func newRing[T any](cap int) ring[T] {
	if cap < 1 {
		cap = 1
	}

	return ring[T]{
		buf:   make([]T, cap),
		isPod: checkPOD[T](),
	}
}

func (r *ring[T]) len() int { return r.size }

func (r *ring[T]) push(v T) {
	if r.closed.Load() {
		return
	}
	if r.size == len(r.buf) {
		r.grow()
	}
	r.buf[r.tail] = v
	r.tail = (r.tail + 1) % len(r.buf)
	r.size++
}

func (r *ring[T]) pop() T {
	var zero T
	r.popping.Store(true)
	if r.closed.Load() {
		r.popping.Store(false)
		return zero
	}
	v := r.buf[r.head]
	r.buf[r.head] = zero
	r.head = (r.head + 1) % len(r.buf)
	r.size--
	r.popping.Store(false)
	return v
}

func (r *ring[T]) grow() {
	newCap := len(r.buf) * 2
	newBuf := make([]T, newCap)

	if r.isPod {
		r.copyUnsafe(newBuf)
	} else {
		r.copySafe(newBuf)
	}

	r.buf = newBuf
	r.head = 0
	r.tail = r.size
}

func (r *ring[T]) close() {
	r.closed.Store(true)
	// Spin while a pop is in progress
	for !r.popping.CompareAndSwap(false, true) {
	}

	// Zero out the buffer so it can be reclaimed by GC
	r.buf = nil
	r.head = 0
	r.tail = 0
	r.size = 0
}

// copySafe is used to copy elements that are or contain pointes or slices
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

// memmove wraps runtime.memmove
//
//go:linkname memmove runtime.memmove
func memmove(dst, src unsafe.Pointer, n uintptr)
