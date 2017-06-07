package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/newhook/workers/db"
	"github.com/newhook/workers/sql"
	"github.com/newhook/workers/workers"
)

var (
	reset    = flag.Bool("reset", false, "Reset")
	traceSQL = flag.Bool("trace-sql", false, "trace sql")
)

func main() {
	flag.Parse()
	if *traceSQL {
		db.TraceSQL = true
	}

	if *reset {
		if err := db.Reset(db.DB()); err != nil {
			panic(err)
		}

		if err := db.MaybeSetupGlobal(db.DB()); err != nil {
			panic(err)
		}
	}

	workers.Configure(workers.Options{
		Concurrency: 100,
	})

	start := time.Now()
	count := 0
	var countMutex sync.Mutex
	workers.Add("test", func(msg *workers.Message) {
		countMutex.Lock()
		count++
		countMutex.Unlock()
		fmt.Println(msg.JID)
		//b, _ := msg.EncodePretty()
		//fmt.Println(string(b))
	})

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		workers.Run(os.Stdout)
	}()

	<-signals

	workers.Stop()
	fmt.Println("processed", count, "jobs in", time.Since(start), float64(count)/time.Since(start).Seconds(), "per second")
}
