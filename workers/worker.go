package workers

import (
	"io"

	"github.com/newhook/workers/db"
	"github.com/newhook/workers/fair"
)

type Job func(msg *Message)

var (
	queues = map[string]Job{}
	ops    = Options{
		Concurrency: 2,
	}
	pool *fair.Pool
)

func Add(queue string, fn Job) {
	queues[queue] = fn
}

type Options struct {
	Concurrency int
}

func Configure(options Options) {
	ops = options
}

func Run(writer io.Writer) {
	var names []string
	for k := range queues {
		names = append(names, k)
	}
	if err := db.PrepareQueues(names); err != nil {
		panic(err)
	}

	concurrencyPool = makeConcurrencyPool(ops.Concurrency)

	pool = fair.New(work, pull, writer)
	pool.Run()
}

func Stop() {
	if pool != nil {
		pool.Shutdown()
		pool = nil
	}

	if concurrencyPool != nil {
		close(concurrencyPool)
	}
}
