package internal

import (
	"container/heap"
	"sync"
)

// The core logic for the scheduler
type Scheduler struct {
	jq JobQueue
	mu sync.Mutex
}

func (s *Scheduler) addJob(j Job) {
	s.mu.Lock()
	defer s.mu.Unlock()

	heap.Push(&s.jq, j)
}
