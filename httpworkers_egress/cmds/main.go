package main

import (
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/newhook/workers/httpworkers_egress/serve"
)

func main() {
	var wg sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer wg.Done()
		for {
			serve.Refresh()
			select {
			case <-done:
			case <-time.After(1 * time.Second):
			}
		}
	}()
	router := httprouter.New()
	router.POST("/fetch/:queue", serve.Fetch)
	router.POST("/ack", serve.Ack)
	router.POST("/ping", serve.Ping)

	// XXX: graceful.
	log.Fatal(http.ListenAndServe(":8080", router))
	close(done)
	wg.Wait()
}
