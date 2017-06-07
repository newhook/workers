package main

import (
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/newhook/workers/httpworkers/serve"
	"github.com/newhook/workers/shard"
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

	go func() {
		shard.Run(os.Stdout)
	}()

	// XXX: graceful.
	log.Fatal(http.ListenAndServe(":8080", router))
	close(done)
	wg.Wait()
}
