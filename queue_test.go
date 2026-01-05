package queue_test

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	queue "github.com/fireflycons/chan-queue"
	"github.com/stretchr/testify/require"
)

func TestEnqueueDequeue(t *testing.T) {

	t.Run("Plain Old Datatype", func(t *testing.T) {
		q := queue.New[int]()

		input := make([]int, 1000)
		output := make([]int, 0, 1000)

		for i := range 1000 {
			input[i] = i
			require.NoError(t, q.Enqueue(i))
		}

		q.Close()

		for v := range q.Dequeue() {
			output = append(output, v)
		}

		require.Equal(t, input, output)
	})

	t.Run("Compound Datatype", func(t *testing.T) {

		type compound struct {
			id   int
			data struct {
				name string
			}
		}

		q := queue.New[compound]()

		input := make([]compound, 1000)
		output := make([]compound, 0, 1000)

		for i := range 1000 {
			v := compound{
				id: i,
				data: struct {
					name string
				}{
					name: fmt.Sprintf("test %03d", i),
				},
			}
			input[i] = v
			require.NoError(t, q.Enqueue(v))
		}

		q.Close()

		for v := range q.Dequeue() {
			output = append(output, v)
		}

		require.Equal(t, input, output)
	})
}

func TestClose(t *testing.T) {

	q := queue.New[int]()

	require.NoError(t, q.Enqueue(1))
	q.Close()
	require.ErrorIs(t, q.Enqueue(1), queue.ErrQueueClosed)
	v := <-q.Dequeue()
	require.Equal(t, 1, v)
	_, ok := <-q.Dequeue()
	require.False(t, ok)
	require.ErrorIs(t, q.Enqueue(1), queue.ErrQueueClosed)
}

func TestDrain(t *testing.T) {

	t.Run("Sequential drain", func(t *testing.T) {
		tests := []int{16}

		for _, tt := range tests {
			t.Run(fmt.Sprintf("Initial elements: %d", tt), func(t *testing.T) {
				q := queue.New[int]()
				for range tt {
					require.NotPanics(t, func() {
						require.NoError(t, q.Enqueue(1))
					}, "Enqueue should not panic")
				}

				q.Close()
				q.Drain()
				require.Equal(t, 0, q.Len())
				err := q.Enqueue(1)
				require.ErrorIs(t, err, queue.ErrQueueClosed)
				_, ok := <-q.Dequeue()
				require.False(t, ok)
			})
		}
	})

	t.Run("Concurrent drain", func(t *testing.T) {
		q := queue.New[int]()

		for range 1_000_000 {
			require.NoError(t, q.Enqueue(1))
		}

		wg := sync.WaitGroup{}
		numPopped := 0

		wg.Go(func() {
			for range q.Dequeue() {
				numPopped++
			}
		})

		time.Sleep(time.Millisecond * 25)

		wg.Go(func() {
			q.Close()
			q.Drain()
		})

		wg.Wait()
		fmt.Println("Number of entries popped before drain:", numPopped)
	})
}

func TestConcurrentEnqueueDequeue(t *testing.T) {

	// Simulate a worker pool situation where something populates the queue while workers
	// pull from it

	const nElements = 10_000

	input := make([]int, nElements)
	output := make([]int, 0, nElements)

	for i := range nElements {
		input[i] = i
	}

	wg := sync.WaitGroup{}
	mu := sync.Mutex{}

	q := queue.New[int]()

	wg.Go(func() {
		for i := range nElements {
			require.NoError(t, q.Enqueue(input[i]))
		}

		q.Close()
	})

	worker := func(id int) {
		fmt.Printf("Worker %d started\n", id)
		cnt := 0
		for v := range q.Dequeue() {
			mu.Lock()
			output = append(output, v)
			mu.Unlock()
			cnt++
			// Simulate work
			d := time.Duration(rand.Intn(20)+1) * time.Microsecond
			time.Sleep(d)
		}
		fmt.Printf("Worker %d stopped. Processed %d elements\n", id, cnt)
	}

	for i := range runtime.NumCPU() {
		wg.Go(func() {
			worker(i)
		})
	}

	wg.Wait()
	require.ElementsMatch(t, input, output)
}

func BenchmarkIntEnqueue(b *testing.B) {

	tests := []int{
		10_000_000,
		16,
		64,
		1024,
	}

	for _, sz := range tests {
		b.Run(fmt.Sprintf("initial capacity %d", sz), func(b *testing.B) {
			q := queue.New[int](queue.WithInitialCapacity(sz))
			runtime.GC()
			for b.Loop() {
				require.NoError(b, q.Enqueue(1))
			}
			l := q.Len()
			q.Close()
			tm := time.Now()
			q.Drain()
			fmt.Printf("Time to drain %d elements: %v\n", l, time.Since(tm))
		})
	}
}

func BenchmarkIntDequeue(b *testing.B) {

	q := queue.New[int](queue.WithInitialCapacity(10_000_000))

	for i := range 10_000_000 {
		require.NoError(b, q.Enqueue(i))
	}

	for b.Loop() {
		<-q.Dequeue()
	}
}
