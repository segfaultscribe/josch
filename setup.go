package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"

	ir "github.com/segfaultscribe/josch/internal"
)

func setup() *http.ServeMux {

	w_count := os.Getenv("WORKER_COUNT")
	w_count_int, err := strconv.Atoi(w_count)
	if err != nil {
		log.Fatal("Failed to read JOB capacity!")
	}

	walPath := os.Getenv("WAL_FILE")
	if walPath == "" {
		log.Fatal("WAL_FILE environment variable is not set")
	}

	_, statErr := os.Stat(walPath)

	isFreshStart := errors.Is(statErr, os.ErrNotExist)

	file, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal("Failed to open or create WAL file: ", err)
	}

	wal := ir.NewWAL(file)
	dispatcher := ir.NewDispatcher(wal)

	if !isFreshStart {
		log.Println("Existing WAL detected. Running crash recovery...")
		wal.ReplayWAL(dispatcher)
		log.Println("Crash recovery complete.")
	} else {
		log.Println("No existing WAL found. Starting fresh system state.")
	}

	dispatcher.SpinUp(w_count_int)
	go dispatcher.StartDispatcher()

	jobHandler := ir.NewJobHandler(dispatcher)

	mux := http.NewServeMux()
	mux.HandleFunc("/submit-job", jobHandler.HandleInjestion)

	return mux
}
