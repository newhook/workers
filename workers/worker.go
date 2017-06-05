package workers

import (
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"

	simplejson "github.com/bitly/go-simplejson"
	"github.com/newhook/workers/fair"
)

type Message struct {
	*simplejson.Json
}

type Job func(msg *Message)

var (
	queues = map[string]Job{}
	ops    = Options{
		Pool: 2,
	}
	wg   sync.WaitGroup
	stop bool

	pool  *fair.Pool
	names []string
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

func Stop() {
	if pool != nil {
		pool.Shutdown()
		pool = nil
	}
}

func work(id string) (bool, error) {
	i := strings.Index(id, ":")
	if i == -1 {
		return false, errors.New("malformed id")
	}
	env, _ := strconv.Atoi(id[:i])
	queue := id[i+1:]
	if err := dowork(env, queue); err != nil {
		return false, err
	}
	return false, nil
}

func Run(writer io.Writer) {
	for k := range queues {
		names = append(names, k)
	}
	pool = fair.New(work, pull, writer)
	pool.Run()
}