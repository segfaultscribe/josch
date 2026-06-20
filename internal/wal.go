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

// Interface helps connect sheduler functionality to the WAL
type RecoveryTarget interface {
	RestoreSystemState(standard []*Job, delayed []*Job, inProcess []*Job)
}

type SnapshotSource interface {
	GetSnapshotRecords() []WALRecord
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

func (w *WAL) ReplayWAL(target RecoveryTarget) {
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
	// convert maps into slices for passing

	var standardJobs, delayedJobs, inProcessJobs []*Job

	for _, job := range standardTemp {
		standardJobs = append(standardJobs, job)
	}
	for _, job := range delayedTemp {
		delayedJobs = append(delayedJobs, job)
	}
	for _, job := range mainQueue {
		inProcessJobs = append(inProcessJobs, job)
	}

	target.RestoreSystemState(standardJobs, delayedJobs, inProcessJobs)

}

func (w *WAL) CompactLogs(source SnapshotSource) error {
	// tempFileName := "wal.log.tmp"

	// snapshotting
	records := source.GetSnapshotRecords()

	// temporary file
	tmpFile, err := os.OpenFile("wal.log.tmp", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer tmpFile.Close()
	encoder := json.NewEncoder(tmpFile)

	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			return err
		}
	}

	tmpFile.Sync()
	tmpFile.Close()

	// atomic swap
	return os.Rename("wal.log.tmp", w.WALfile.Name())

}
