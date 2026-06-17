package internal

import (
	"log"
)

func (s *Scheduler) worker(id int) {
	for job := range s.JobChannel {
		s.executeJob(&job, id)
	}
}

func (s *Scheduler) executeJob(job *Job, id int) {
	defer func() {
		if r := recover(); r != nil {
			// Use log/slog to log the error and worker stack trace cleanly
			log.Printf("[Worker %d] Panic caught executing job %s: %v", id, job.ID, r)
		}

		log.Printf("[Worker %d] Successfully completed job: %s", id, job.ID)
	}()
}

func (s *Scheduler) SpinUp(workerCount int) {
	for i := 1; i <= workerCount; i++ {
		go s.worker(i)
	}
}
