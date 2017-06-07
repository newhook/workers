package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	simplejson "github.com/bitly/go-simplejson"
	workers "github.com/jrallison/go-workers"
	"github.com/newhook/workers/fair"
	"github.com/newhook/workers/sql"
)

type Fetcher struct {
	queue        string
	ready        chan bool
	finishedwork chan bool
	messages     chan workers.Msgs
	inflight     map[string]msg
	flightLock   sync.Mutex
	wait         int
	stop         chan bool
	exit         chan bool
	pingCancel   chan struct{}
	closed       int32 // atomic int
	retry        workers.Fetcher
	jidSrc       rand.Source

	pool *fair.Pool
}

func (f *Fetcher) Queue() string {
	return f.queue
}

type msg struct {
	env int
	job sql.Job
}

func work(messages chan msg) func(w fair.Work) (bool, error) {
	return func(w fair.Work) (bool, error) {
		d := w.Data.(*sql.Worker)
		env := d.ID
		queue := d.Queue

		job, ok, err := sql.ClaimJob(env, queue)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}

		messages <- msg{env: env, job: job}
		return true, nil
	}
}

func pull(queue string) func() []fair.Work {
	return func() []fair.Work {
		workers, err := sql.FindReadyQueue(queue)
		if err != nil {
			log.Println("FindReady", err)
			return nil
		}
		fmt.Println(workers)
		ids := make([]fair.Work, len(workers))
		for i, w := range workers {
			ids[i] = fair.Work{
				ID:   strconv.Itoa(w.ID) + ":" + w.Queue,
				Data: &w,
			}
		}
		return ids
	}
}

func (f *Fetcher) Fetch() {
	messages := make(chan msg)

	//go f.retry.Fetch()

	go f.ping()

	pool := fair.New(work(messages), pull(f.queue), os.Stdout)
	go func() {
		duration := time.Second
		for {
			if f.Closed() {
				break
			}

			<-f.Ready()
			fmt.Println()
			pool.Step()

			select {
			case <-f.FinishedWork():
			case <-time.After(duration):
			}
		}
	}()

loop:
	for {
		select {
		case msg := <-messages:
			f.sendMessage(msg)
		case <-f.stop:
			atomic.StoreInt32(&f.closed, 1)
			close(f.pingCancel)
			f.exit <- true
			break loop
		}
	}
}

func (f *Fetcher) sendMessage(m msg) {
	msgs := make(workers.Msgs, 0, 1)

	b := make([]byte, 24)
	f.flightLock.Lock()
	jid := generateJid(f.jidSrc, b)
	f.inflight[jid] = m

	data, _ := simplejson.NewJson(m.job.Data)
	msgs = append(msgs, workers.NewMsg(jid, data, m.job.Data))
	f.flightLock.Unlock()

	f.Messages() <- msgs
}

func (f *Fetcher) Acknowledge(messages workers.Msgs) {
	flights := make([]msg, len(messages))
	oks := make([]bool, len(messages))

	f.flightLock.Lock()

	for i, msg := range messages {
		flights[i], oks[i] = f.inflight[msg.Jid()]
	}

	f.flightLock.Unlock()

	for i, msg := range messages {
		if oks[i] {
			if _, err := sql.DeleteJob(flights[i].env, flights[i].job); err != nil {
				log.Println(err)
			}
			f.flightLock.Lock()
			delete(f.inflight, msg.Jid())
			f.flightLock.Unlock()
		} else {
			f.retry.Acknowledge(workers.Msgs{msg})
		}
	}
}

func (f *Fetcher) Ready() chan bool {
	return f.ready
}

func (f *Fetcher) FinishedWork() chan bool {
	return f.finishedwork
}

func (f *Fetcher) Messages() chan workers.Msgs {
	return f.messages
}

func (f *Fetcher) Close() {
	//f.retry.Close()
	f.stop <- true
	<-f.exit
}

func (f *Fetcher) Closed() bool {
	return atomic.LoadInt32(&f.closed) != 0
}

func (f *Fetcher) ping() {
	for {
		select {
		case <-f.pingCancel:
		case <-time.After(30 * time.Second):
		}

		if f.Closed() {
			break
		}

		f.flightLock.Lock()
		for _, msg := range f.inflight {
			if _, err := sql.RefreshJob(msg.env, msg.job); err != nil {
				log.Println(err)
			}
		}
		f.flightLock.Unlock()
	}
}

func newFetcher(queue string, n, resendTimeframe int) workers.Fetcher {
	return &Fetcher{
		queue:        queue,
		ready:        make(chan bool),
		finishedwork: make(chan bool),
		messages:     make(chan workers.Msgs),
		inflight:     make(map[string]msg),
		wait:         resendTimeframe,
		stop:         make(chan bool),
		exit:         make(chan bool),
		pingCancel:   make(chan struct{}),
		closed:       0,
		jidSrc:       rand.NewSource(time.Now().UnixNano()),
	}
}

// hex characters only pls.
const (
	letterBytes   = "abcdef0123456789"   // All the hex characters.
	letterIdxBits = 4                    // 4 bits to represent a letter index (2^4 == 16)
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func generateJid(source rand.Source, b []byte) string {
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	n := len(b)
	for i, cache, remain := n-1, source.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = source.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

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
		return newFetcher(queueName, 1, 60)
	}

	workers.Process(queueName, func(m workers.Msgs) {
		log.Println("msg", m)
	}, 2)

	workers.Run()
}
