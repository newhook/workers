package shard

import (
	"io"
	"log"
	"sync"
	"time"

	"github.com/newhook/workers/sql"
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
			if err := sql.ProcessRetries(); err != nil {
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
			if err := sql.ProcessInflight(); err != nil {
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
