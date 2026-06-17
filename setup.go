package controller

import (
	"log"
	"net/http"
	"os"
	"strconv"

	ir "github.com/segfaultscribe/josch/internal"
)

func setup() *http.ServeMux {
	dispatcher := ir.NewDispatcher()
	w_count := os.Getenv("WORKER_COUNT")
	w_count_int, err := strconv.Atoi(w_count)
	if err != nil {
		log.Fatal("Failed to read JOB capacity!")
	}

	dispatcher.SpinUp(w_count_int)

	go dispatcher.StartDispatcher()

	jobHandler := ir.NewJobHandler(dispatcher)

	mux := http.NewServeMux()
	mux.HandleFunc("/submit-job", jobHandler.HandleInjestion)

	return mux
}
