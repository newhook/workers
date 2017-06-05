package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/newhook/workers/db"
	"github.com/newhook/workers/sql"
	"github.com/newhook/workers/workers"
)

var (
	reset    = flag.Bool("reset", false, "Reset")
	queue    = flag.Bool("queue", false, "queue jobs")
	env      = flag.Int("env", 1, "environment")
	traceSQL = flag.Bool("trace-sql", false, "trace sql")
)

func main() {
	flag.Parse()
	if *traceSQL {
		db.TraceSQL = true
	}

	if *reset {
		if err := sql.Reset(db.DB()); err != nil {
			panic(err)
		}

		if err := sql.MaybeSetupGlobal(db.DB()); err != nil {
			panic(err)
		}
	}

	if *queue {
		if ids, err := sql.EnvironmentIDs(db.DB()); err != nil {
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
				if err := sql.SetupEnv(db.DB(), *env); err != nil {
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
			case <-time.After(1 * time.Second):
			}
		}
		return
	}

	workers.Configure(workers.Options{})

	workers.Add("test", func(msg *workers.Message) {
		fmt.Println("Start: ", msg.JID)
		if msg.RetryAttempt() > 0 {
			fmt.Println("retry", msg.RetryAttempt())
		}
		b, _ := msg.EncodePretty()
		fmt.Println(string(b))

		count := msg.Get("count").MustInt()
		if count%2 == 0 && msg.RetryAttempt() == 0 {
			panic("fail this please!")
		}
	})

	workers.Run(os.Stdout)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	<-signals

	workers.Stop()
}
