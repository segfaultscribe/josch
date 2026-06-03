package internal

type PrioritizedJob struct {
	JobData Job
	index   int
}

type JobQueue []*PrioritizedJob

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
