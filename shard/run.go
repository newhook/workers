package shard

import (
	"io"
	"log"
	"sync"
	"time"

	"github.com/newhook/workers/db"
)

var (
	wg   sync.WaitGroup
	done chan (struct{})
)

func Run(writer io.Writer) {
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := db.ProcessRetries(); err != nil {
				log.Println(err)
			}

			select {
			case <-done:
				return
			case <-time.After(1 * time.Second):
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := db.ProcessInflight(); err != nil {
				log.Println(err)
			}

			select {
			case <-done:
				return
			case <-time.After(1 * time.Second):
			}
		}
	}()
}

func Stop() {
	if done != nil {
		close(done)
		wg.Wait()
	}
}
