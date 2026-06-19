package internal

import (
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

// The core logic for the scheduler
type Scheduler struct {
	sjq           *tsWrapJobQueue
	mu            sync.Mutex
	immJobChannel chan Job
	JobChannel    chan Job
	activeJobs    map[ulid.ULID]*WALRecord
	maxJobs       int
	readySignal   chan struct{} // wake up dispatcher
	stopChannel   chan struct{} // graceful shutdown
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
		sjq:           NewSafeJobQueue(),
		maxJobs:       maxJobsInt,
		JobChannel:    make(chan Job, workerPoolSizeInt),
		immJobChannel: make(chan Job, workerPoolSizeInt),
		activeJobs:    make(map[ulid.ULID]*WALRecord),
		readySignal:   make(chan struct{}, 1),
		stopChannel:   make(chan struct{}),
	}
	return s
}

func (s *Scheduler) AddJob(j Job) bool {
	now := time.Now()
	// CASE: Immediate job
	if j.RunAt.IsZero() || j.RunAt.Before(now) || j.RunAt.Equal(now) {

		if len(s.immJobChannel) >= s.maxJobs {
			return false
		}

		select {
		case s.immJobChannel <- j:
			wEntry := &WALRecord{
				RecordId: ulid.Make(),
				JData:    &j,
				EntryAt:  time.Now(),
				Event:    EventScheduledStandard,
			}
			s.activeJobs[j.ID] = wEntry
			return true
		default:
			return false
		}
	}

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

func (s *Scheduler) StartDispatcher() {
	// The role of the dispatcher is to act as a layer of control
	// between the data and the worker pool

	var timer *time.Timer
	// the dispatcher loop
	for {

		// the condition handles when the queue is empty
		pjob, exists := s.sjq.Peek()
		if !exists {
			// QUEUE IS EMPTY
			// block using select
			select {
			case <-s.readySignal:
				continue
			case <-s.stopChannel:
				return
			}
		}

		// the queue is not empty and there are jobs to be processed
		// pjq is the next Job
		if pjob.JobData.RunAt.Before(time.Now()) || pjob.JobData.RunAt.Equal(time.Now()) {
			pjobPopped, ok := s.sjq.Pop()
			if ok {
				s.JobChannel <- pjobPopped.JobData
				wEntry := &WALRecord{
					RecordId: ulid.Make(),
					JData:    &pjob.JobData,
					EntryAt:  time.Now(),
					Event:    EventInProcess,
				}
				s.activeJobs[wEntry.RecordId] = wEntry
			}
			continue
		}

		// if the top job hasn't yet reached execution time
		// we need the dispatcher to sleep BUT also
		// wake up if a job is added to the queue

		sleepDuration := pjob.JobData.RunAt.Sub(time.Now())
		if timer == nil {
			timer = time.NewTimer(sleepDuration)
		} else {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(sleepDuration)
		}

		select {
		case imJob := <-s.immJobChannel:
			s.JobChannel <- imJob
		case <-timer.C:
		case <-s.readySignal:
		case <-s.stopChannel:
			timer.Stop()
			return
		}

	}
}
