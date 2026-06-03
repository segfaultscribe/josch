package internal

import (
	"time"

	"github.com/oklog/ulid/v2"
)

type JobStatus string

const (
	StatusPending   JobStatus = "PENDING"
	StatusCompleted JobStatus = "COMPLETED"
	StatusFailed    JobStatus = "FAILED"
)

type Job struct {
	ID        ulid.ULID `json:"id"`
	Type      string    `json:"type"`
	Payload   []byte    `json:"payload"`
	RunAt     time.Time `json:"run_at"`
	Status    JobStatus `json:"status"`
	Retries   int       `json:"retries"`
	CreatedAt time.Time `json:"created_at"`
}
