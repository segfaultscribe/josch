// file to handle api endpoints
package controller

import (
	"log"
	"net/http"
	"time"

	"github.com/joho/godotenv"
)

func main() {

	srv := &http.Server{
		Addr:         ":5000",
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	srv.ListenAndServe()
}
