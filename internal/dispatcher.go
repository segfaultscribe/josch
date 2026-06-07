package internal

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

// The core logic for the scheduler
type Scheduler struct {
	sjq         *tsWrapJobQueue
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
		sjq:         NewSafeJobQueue(),
		maxJobs:     maxJobsInt,
		JobChannel:  make(chan Job, workerPoolSizeInt),
		readySignal: make(chan struct{}, 1),
		stopChannel: make(chan struct{}),
	}
	return s
}

func (s *Scheduler) AddJob(j Job) bool {
	pjb := &PrioritizedJob{
		JobData: j,
		index:   -1,
	}

	clear := s.sjq.Push(pjb)
	if !clear {
		return false // queue is full MAX_JOB limit hit
	}

	select {
	case s.readySignal <- struct{}{}:
	default:
	}

	return true
}

func (s *Scheduler) startDispatcher() {
	// The role of the dispatcher is to act as a layer of control
	// between the data and the worker pool

	var timer *time.Timer
	// the dispatcher loop
	for {
		s.mu.Lock()

		// the condition handles when the queue is empty
		if len(s.jq) == 0 {
			fmt.Printf("Queue is empty!")
			s.mu.Unlock()

			// block using select
			select {
			case <-s.readySignal:
				continue
			case <-s.stopChannel:
				return
			}
		}

		// the queue is not empty and there are jobs to be processed
		nextJob := s.jq[0]
	}
}
