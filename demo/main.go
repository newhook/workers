package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/newhook/workers/db"
	wshard "github.com/newhook/workers/shard"
	"github.com/newhook/workers/sql"
	"github.com/newhook/workers/workers"
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
			if id, err := workers.Queue(*env, "test", map[string]interface{}{
				"hello": "world",
				"count": count,
				"sleep": 10,
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

	if *shard {
		go func() {
			wshard.Run(os.Stdout)
		}()
	}

	//workers.Configure(workers.Options{})

	workers.Add("test", func(msg *workers.Message) {
		b, _ := msg.Encode()
		log.Println("start:", msg.JID, ":", string(b))
		if msg.RetryAttempt() > 0 {
			fmt.Println("retry", msg.RetryAttempt())
		}

		//count := msg.Get("count").MustInt()
		//if count%2 == 0 && msg.RetryAttempt() == 0 {
		//panic("fail this please!")
		//}
		sleep := msg.Get("sleep").MustInt()
		log.Println(msg.JID, ": ->sleep")
		select {
		case <-msg.Ctx.Done():
			log.Println(msg.JID, ": job was canceled")
			return
		case <-time.After(time.Duration(sleep) * time.Second):
		}
		log.Println(msg.JID, ": <-sleep")
	})

	workers.Run(os.Stdout)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	<-signals

	workers.Stop()

	if *shard {
		wshard.Stop()
	}
}
