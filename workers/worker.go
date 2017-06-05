package workers

import (
	"io"
	"log"
	"sync"
	"time"

	"github.com/newhook/workers/fair"
	"github.com/newhook/workers/sql"
)

type Job func(msg *Message)

var (
	queues = map[string]Job{}
	ops    = Options{
		Pool: 2,
	}
	pool *fair.Pool
)

func Add(queue string, fn Job) {
	queues[queue] = fn
}

type Options struct {
	Pool int
}

func Configure(options Options) {
	ops = options
}

func work(w fair.Work) (bool, error) {
	d := w.Data.(*sql.Worker)
	if err := dowork(d.ID, d.Queue); err != nil {
		return false, err
	}
	d.Count--
	return d.Count > 0, nil
}

var (
	wg   sync.WaitGroup
	done chan (struct{})
)

func processRetries(done chan struct{}, queues []string) {
	for {
		if err := sql.ProcessRetries(queues); err != nil {
			log.Println(err)
		}

		select {
		case <-done:
			return
		case <-time.After(1 * time.Second):
		}
	}
}

func Run(writer io.Writer) {
	var names []string
	for k := range queues {
		names = append(names, k)
	}
	if err := sql.PrepareQueues(names); err != nil {
		panic(err)
	}

	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		processRetries(done, names)
	}()

	pool = fair.New(work, pull, writer)
	pool.Run()
}

func Stop() {
	if pool != nil {
		pool.Shutdown()
		pool = nil
	}
	if done != nil {
		close(done)
		wg.Wait()
	}
}
