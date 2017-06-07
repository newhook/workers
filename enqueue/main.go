package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/newhook/workers/db"
	"github.com/newhook/workers/egress"
)

var (
	reset    = flag.Bool("reset", false, "Reset")
	queue    = flag.Bool("queue", false, "queue jobs")
	shard    = flag.Bool("shard", false, "run shard level jobs")
	env      = flag.Int("env", 1, "environment")
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

	if ids, err := db.EnvironmentIDs(db.DB()); err != nil {
		panic(err)
	} else {
		found := false
		for _, id := range ids {
			if *env == id {
				found = true
				break
			}
		}
		if !found {
			fmt.Println("initializing database for env", *env)
			if err := db.SetupEnv(db.DB(), *env); err != nil {
				panic(err)
			}
		}
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	count := 0
loop:
	for {
		if err := egress.QueueMsgTx(*env, db.DB(), map[string]interface{}{
			"type":           "webhook",
			"environment_id": *env,
			"data": map[string]interface{}{
				"hello": "world",
				"count": count,
				"sleep": 10,
			},
		}); err != nil {
			panic(err)
		}
		count++
		select {
		case <-signals:
			break loop
		case <-time.After(1 * time.Second):
		}
	}
	return
}
