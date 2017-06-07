package main

import (
	"log"
	"os"
	"strconv"
	"time"

	workers "github.com/jrallison/go-workers"
	"github.com/newhook/workers/httpworkers"
)

func main() {
	numWorkers := 2
	queueName := "test"
	writer := os.Stdout
	workers.Logger = log.New(writer, "workers: ", log.LstdFlags)
	workers.Configure(map[string]string{
		"server":   "localhost:6379",
		"database": "0",
		"pool":     strconv.Itoa(numWorkers * 3 / 4),
		"process":  "1",
	})
	workers.Config.Fetch = func(queue string) workers.Fetcher {
		//return fetcher.New(queueName, 1, 60)
		return httpworkers.New(queueName, 1, 60)
	}

	workers.Process(queueName, func(msgs workers.Msgs) {
		msg := msgs[0]
		data := msg.Args()
		sleep := data.Get("sleep").MustInt()
		_ = sleep
		log.Println(msg.Jid(), ": ->sleep")
		//time.Sleep(time.Duration(sleep) * time.Second)
		time.Sleep(time.Second)
		log.Println(msg.Jid, ": <-sleep")
	}, 2)

	workers.Run()
}
