package internal

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/oklog/ulid/v2"
)

type WALEvent string

const (
	EventScheduled WALEvent = "SCHEDULED"
	EventInProcess WALEvent = "IN-PROCESS"
	EventDone      WALEvent = "DONE"
)

type WALRecord struct {
	RecordId ulid.ULID `json:"record_id"`
	JData    *Job      `json:"job_data,omitempty"`
	EntryAt  time.Time `json:"timestamp"`
	Event    WALEvent  `json:"event"`
}

func AddWALRecord(wr *WALRecord) {
	walFile := os.Getenv("WAL_FILE")

	file, err := os.OpenFile(walFile, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal("Cannot open WAL file!")
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(wr)
	if err != nil {
		log.Fatal("Error writing WAL record to file!")
	}
}

func ReplayWAL() {

}

func CompactLogs() {

}
