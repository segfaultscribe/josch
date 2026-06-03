// file to handle api endpoints
package controller

import (
	"net/http"
	"time"
)

func main() {

	srv := &http.Server{
		Addr:         ":5000",
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	srv.ListenAndServe()
}
