package httpworkers

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	simplejson "github.com/bitly/go-simplejson"
	workers "github.com/jrallison/go-workers"
	"github.com/newhook/workers/fair"
	"github.com/newhook/workers/httpworkers/client"
	"github.com/newhook/workers/httpworkers/data"
)

type Fetcher struct {
	queue        string
	ready        chan bool
	finishedwork chan bool
	messages     chan workers.Msgs
	inflight     map[string]data.Job
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

func (f *Fetcher) Fetch() {
	messages := make(chan data.Job)

	//go f.retry.Fetch()

	go f.ping()

	go func() {
		duration := time.Second
		for {
			if f.Closed() {
				break
			}

			<-f.Ready()
			job, ok, err := client.Fetch(f.queue)
			if err != nil {
				fmt.Println("error", err)
			} else if ok {
				fmt.Println("sending job", job)
				messages <- job
			}

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

func (f *Fetcher) sendMessage(m data.Job) {
	msgs := make(workers.Msgs, 0, 1)

	b := make([]byte, 24)
	f.flightLock.Lock()
	jid := generateJid(f.jidSrc, b)
	f.inflight[jid] = m

	data, _ := simplejson.NewJson(m.Data)
	msgs = append(msgs, workers.NewMsg(jid, data, m.Data))
	f.flightLock.Unlock()

	f.Messages() <- msgs
}

func (f *Fetcher) Acknowledge(messages workers.Msgs) {
	flights := make([]data.Job, len(messages))
	oks := make([]bool, len(messages))

	f.flightLock.Lock()

	for i, msg := range messages {
		flights[i], oks[i] = f.inflight[msg.Jid()]
	}

	f.flightLock.Unlock()

	for i, msg := range messages {
		if oks[i] {
			if _, err := client.Ack(flights[i]); err != nil {
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
		for _, job := range f.inflight {
			if _, err := client.Ping(job); err != nil {
				log.Println(err)
			}
		}
		f.flightLock.Unlock()
	}
}

func New(queue string, n, resendTimeframe int) workers.Fetcher {
	return &Fetcher{
		queue:        queue,
		ready:        make(chan bool),
		finishedwork: make(chan bool),
		messages:     make(chan workers.Msgs),
		inflight:     make(map[string]data.Job),
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
