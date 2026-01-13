package main

import (
	"fmt"

	queue "github.com/fireflycons/concurrentqueue"
)

func main() {
	q := queue.New[int]()

	for i := range 10 {
		_ = q.Enqueue(i)
	}

	// Close the queue to signal no more elements will be added
	q.Close()

	// Dequeue is a channel, therefore it would block until elements are available
	// or the queue is closed and emptied.
	for v := range q.Dequeue() {
		fmt.Println(v)
	}
}
