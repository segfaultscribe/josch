// file to handle api endpoints
package main

import (
	"log"
	"net/http"
	"time"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	router := setup()

	srv := &http.Server{
		Addr:         ":5000",
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	srv.ListenAndServe()

	log.Println("Server booting up on port :5000...")
	log.Fatal(srv.ListenAndServe())
}
