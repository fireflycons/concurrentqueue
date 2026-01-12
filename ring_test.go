package queue

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntRing(t *testing.T) {

	const nElements = 10000
	input := make([]int, nElements)
	output := make([]int, 0, nElements)

	r := newRing[int](16)
	fmt.Println(&r)
	for i := range nElements {
		input[i] = i
		r.push(i)
	}

	assert.Equal(t, nElements, r.len(), "unexepected number of items in ring")
	fmt.Println(&r)

	for range nElements {
		output = append(output, r.pop())
	}

	assert.Equal(t, 0, r.len(), "unexepected number of items in ring")
	require.Equal(t, input, output)
	fmt.Println(&r)
}

func TestAppendTo(t *testing.T) {

	const nElements = 10_000

	t.Run("Sequential AppendTo", func(t *testing.T) {
		r1 := newRing[int](16)
		r2 := newRing[int](16)

		for i := range nElements {
			r1.push(i)
			r2.push(i)
		}

		r1.appendTo(&r2)

		assert.Equal(t, nElements*2, r2.len(), "unexpected number of items in ring")
		for i := range nElements * 2 {
			v := i % nElements
			assert.Equal(t, v, r2.pop(), "unexpected value in ring")
		}

		r1.close()
		r2.close()
	})

	t.Run("Concurrent AppendTo", func(t *testing.T) {

		// Verify that we can safely append to a ring while another goroutine is pushing to it
		r1 := newRing[int](16)
		r2 := newRing[int](16)

		var lock atomic.Bool

		expected := make([]int, nElements*2)
		for i := range nElements * 2 {
			expected[i] = i
		}

		for i := range nElements {
			r1.push(i)
		}

		wg := sync.WaitGroup{}

		lock.Store(true)

		wg.Go(func() {
			// Give time for second goroutine to start and reach the lock
			time.Sleep(500 * time.Millisecond)

			for i := range nElements {
				r2.push(i + nElements)
				if i == 100 {
					// Release the lock to allow appendTo to proceed
					lock.Store(false)
				}
			}
		})
		wg.Go(func() {
			for !lock.CompareAndSwap(false, true) {
			}
			r1.appendTo(&r2)
		})
		wg.Wait()

		assert.Equal(t, nElements*2, r2.len(), "unexpected number of items in ring")

		actual := make([]int, 0, nElements*2)
		for r2.len() > 0 {
			actual = append(actual, r2.pop())
		}

		require.ElementsMatch(t, expected, actual, "unexpected values in ring")
	})
}

func BenchmarkIntRingPush(b *testing.B) {

	tests := []int{
		16,
		64,
		1024,
	}

	for _, sz := range tests {
		b.Run(fmt.Sprintf("initial capacity %d", sz), func(b *testing.B) {
			r := newRing[int](sz)
			for b.Loop() {
				r.push(1)
			}
		})
	}
}

func BenchmarkStringRingPush(b *testing.B) {

	tests := []int{
		16,
		64,
		1024,
	}

	for _, sz := range tests {
		b.Run(fmt.Sprintf("initial capacity %d", sz), func(b *testing.B) {
			r := newRing[string](sz)
			for b.Loop() {
				r.push("test")
			}
		})
	}
}
