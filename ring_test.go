package queue

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntRing(t *testing.T) {

	input := make([]int, 1000)
	output := make([]int, 0, 1000)

	r := newRing[int](16)
	for i := range 1000 {
		input[i] = i
		r.push(i)
	}

	assert.Equal(t, 1000, r.len(), "unexepected number of items in ring")

	for range 1000 {
		output = append(output, r.pop())
	}

	assert.Equal(t, 0, r.len(), "unexepected number of items in ring")
	require.Equal(t, input, output)
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
			b.ResetTimer()
			for range b.N {
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
			b.ResetTimer()
			for range b.N {
				r.push("test")
			}
		})
	}
}
