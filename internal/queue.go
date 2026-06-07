package internal

import (
	"container/heap"
	"log"
	"os"
	"strconv"
	"sync"
)

type PrioritizedJob struct {
	JobData Job
	index   int
}

type JobQueue []*PrioritizedJob

type tsWrapJobQueue struct {
	mu      sync.Mutex
	jq      JobQueue // This holds your raw slice heap
	maxJobs int
}

func (jq JobQueue) Len() int {
	return len(jq)
}

func (jq *JobQueue) Push(x any) {
	n := len(*jq)
	job := x.(*PrioritizedJob)
	job.index = n
	*jq = append(*jq, job)
}

func (jq *JobQueue) Pop() any {
	old := *jq
	n := len(old)
	job := old[n-1]
	old[n-1] = nil
	job.index = -1
	*jq = old[0 : n-1]
	return job
}

func (jq JobQueue) Less(i, j int) bool {
	return jq[i].JobData.RunAt.Before(jq[j].JobData.RunAt)
}

func (jq JobQueue) Swap(i, j int) {
	jq[i], jq[j] = jq[j], jq[i]
	jq[i].index = i
	jq[j].index = j
}

// thread saftey stuff

func NewSafeJobQueue() *tsWrapJobQueue {
	mj := os.Getenv("MAX_JOBS")
	mjInt, err := strconv.Atoi(mj)

	if err != nil {
		log.Fatalf("Cannot find MAX JOB limit!")
	}

	sjq := &tsWrapJobQueue{
		jq:      make(JobQueue, 0),
		maxJobs: mjInt,
	}

	heap.Init(&sjq.jq)
	return sjq
}

func (t *tsWrapJobQueue) Push(j *PrioritizedJob) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.jq) >= t.maxJobs {
		return false
	}
	heap.Push(&t.jq, j)
	return true
}

func (t *tsWrapJobQueue) Pop() (*PrioritizedJob, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.jq) == 0 {
		return nil, false
	}

	job := heap.Pop(&t.jq).(*PrioritizedJob)
	return job, true
}

func (t *tsWrapJobQueue) Peek() (*PrioritizedJob, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.jq) == 0 {
		return nil, false
	}

	return t.jq[0], true
}

func (t *tsWrapJobQueue) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	return len(t.jq)
}
