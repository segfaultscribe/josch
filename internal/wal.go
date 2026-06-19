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
	EventScheduledStandard WALEvent = "SCHEDULED_STANDARD"
	EventScheduledDelayed  WALEvent = "SCHEDULED_DELAYED"
	EventInProcess         WALEvent = "IN-PROCESS"
	EventDone              WALEvent = "DONE"
)

type WALRecord struct {
	RecordId ulid.ULID `json:"record_id"`
	JData    *Job      `json:"job_data,omitempty"`
	EntryAt  time.Time `json:"timestamp"`
	Event    WALEvent  `json:"event"`
}

type WAL struct {
	WALfile *os.File
}

func NewWAL(f *os.File) *WAL {
	return &WAL{
		WALfile: f,
	}
}

func (w *WAL) AddWALRecord(wr *WALRecord) {
	// ADDS a WAL record into the current WAL File

	file, err := os.OpenFile(w.WALfile.Name(), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("Cannot open WAL file")
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(wr)
	if err != nil {
		log.Fatal("Error writing WAL record to file")
	}
}

func (w *WAL) ReplayWAL(s *Scheduler) {
	var logs []WALRecord
	file, err := os.OpenFile(w.WALfile.Name(), os.O_RDONLY, 0)
	if err != nil {
		log.Fatal("Error opening WAL File for replay")
	}

	decoder := json.NewDecoder(file)

	for decoder.More() {
		var entry WALRecord
		err = decoder.Decode(&entry)
		if err != nil {
			log.Fatal("Error decoding WAL line")
		}
		logs = append(logs, entry)
	}
	err = decoder.Decode(&logs)
	if err != nil {
		log.Fatal("Error decoding WAL!")
	}

	standardTemp := make(map[ulid.ULID]*Job)
	delayedTemp := make(map[ulid.ULID]*Job)
	mainQueue := make(map[ulid.ULID]*Job)

	for _, entry := range logs {
		switch entry.Event {
		case "SCHEDULED_STANDARD":
			standardTemp[entry.RecordId] = entry.JData
		case "SCHEDULED_DELAYED":
			delayedTemp[entry.RecordId] = entry.JData
		case "IN-PROCESS":
			mainQueue[entry.RecordId] = entry.JData
		case "DONE":
			delete(standardTemp, entry.RecordId)
			delete(delayedTemp, entry.RecordId)
			delete(mainQueue, entry.RecordId)
		}
	}
	// after the loop the three queues contain the state of the system before crash
	for _, job := range standardTemp {
		s.immJobChannel <- *job
	}

	for _, job := range delayedTemp {
		pjb := &PrioritizedJob{
			JobData: *job,
			index:   -1,
		}

		clear := s.sjq.Push(pjb)

		if !clear {
			log.Fatal("Failed to add job to delayed queue!")
		}
	}

	for _, job := range mainQueue {
		s.JobChannel <- *job
	}

	select {
	case s.readySignal <- struct{}{}:
	default:
	}

}

func (w *WAL) CompactLogs() {
	// tempFileName := "wal.log.tmp"

	// snapshotting

}
