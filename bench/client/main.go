package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/newhook/workers/db"
	"github.com/newhook/workers/sql"
	"github.com/newhook/workers/workers"
)

var (
	env      = flag.Int("env", 1, "environment")
	traceSQL = flag.Bool("trace-sql", false, "trace sql")
)

func main() {
	flag.Parse()
	if *traceSQL {
		db.TraceSQL = true
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
		if id, err := workers.Queue(1, "test", map[string]interface{}{
			"hello": "world",
			"count": count,
		}); err != nil {
			panic(err)
		} else {
			fmt.Println("queued job", id)
		}
		count++
		select {
		case <-signals:
			break loop
		default:
		}
	}
}
