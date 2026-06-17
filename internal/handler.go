package internal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/oklog/ulid/v2"
)

type CreateJobRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
	RunAt   time.Time       `json:"run_at"`
}

type Response struct {
	Id     string `json:"jobId"`
	Status string `json:"status"`
}

type JobHandler struct {
	scheduler *Scheduler
}

func NewJobHandler(sched *Scheduler) *JobHandler {
	return &JobHandler{
		scheduler: sched,
	}
}

func (jh *JobHandler) HandleInjestion(w http.ResponseWriter, r *http.Request) {
	//extract the request data
	var req CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	//create the Job
	job := Job{
		ID:        ulid.Make(),
		Type:      req.Type,
		Payload:   req.Payload,
		RunAt:     req.RunAt,
		Status:    JobStatus("PENDING"),
		Retries:   0,
		CreatedAt: time.Now(),
	}
	fmt.Println(job)

	ok := jh.scheduler.AddJob(job)
	if !ok {
		http.Error(w, "Scheduler queue capacity reached", http.StatusServiceUnavailable)
		return
	}

	ret := &Response{
		Id:     job.ID.String(),
		Status: string(job.Status),
	}

	w.Header().Set("Content-type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(ret)
}
