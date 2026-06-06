package internal

import (
	"container/heap"
	"log"
	"os"
	"strconv"
	"sync"
)

// The core logic for the scheduler
type Scheduler struct {
	jq          JobQueue
	mu          sync.Mutex
	JobChannel  chan Job
	maxJobs     int
	readySignal chan struct{} // wake up dispatcher
	stopChannel chan struct{} // graceful shutdown
}

func NewDispatcher() *Scheduler {

	maxJobs := os.Getenv("MAX_JOBS")
	maxJobsInt, err := strconv.Atoi(maxJobs)

	if err != nil {
		log.Fatal("Failed to read JOB capacity!")
	}

	workerPoolSize := os.Getenv("WORKER_POOL_SIZE")
	workerPoolSizeInt, err := strconv.Atoi(workerPoolSize)

	if err != nil {
		log.Fatal("Failed to read WORKER POOL capacity!")
	}

	s := &Scheduler{
		jq:          JobQueue{},
		maxJobs:     maxJobsInt,
		JobChannel:  make(chan Job, workerPoolSizeInt),
		readySignal: make(chan struct{}, 1),
		stopChannel: make(chan struct{}),
	}
	heap.Init(&s.jq)
	return s
}

func (s *Scheduler) AddJob(j Job) {
	heap.Push(&s.jq, j)
}
