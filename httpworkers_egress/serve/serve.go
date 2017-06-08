package serve

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"encoding/json"

	"github.com/julienschmidt/httprouter"
	"github.com/newhook/workers/db"
	"github.com/newhook/workers/egress"
	"github.com/newhook/workers/httpworkers_egress/data"
)

const (
	batchSize    = 10
	inflightTime = 60
)

type inflightEgress struct {
	*egress.Egress
	expiry time.Time
}

type envState struct {
	env   int            // The env id.
	qtype db.EgressQueue // The queue type.

	// Highest entry in the database.
	inserted int

	// Next id to fetch.
	processed int

	// Set of inflights messages for this env/queue.
	inflights map[int]*inflightEgress

	// Set of pending messages pulled from the database
	// but not yet inflight.
	queue []*egress.Egress
}

func (e *envState) pull() (*egress.Egress, error) {
	now := time.Now()
	var expired []*egress.Egress
	for _, msg := range e.inflights {
		if now.After(msg.expiry) {
			log.Println("expiring inflight message", msg.ID)
			expired = append(expired, msg.Egress)
		}
	}

	if len(expired) > 0 {
		if err := egress.RenewToken(e.env, e.qtype, expired); err != nil {
			return nil, err
		}

		for _, msg := range expired {
			delete(e.inflights, msg.ID)
			e.queue = append(e.queue, msg)
		}
	}

	if len(e.queue) == 0 && e.processed < e.inserted {
		log.Println("pulln", e.env, e.queue, e.processed)
		if data, max, err := egress.Pulln(e.env, e.qtype, e.processed, batchSize); err != nil {
			return nil, err
		} else {
			log.Printf("%d:%d: pulled %d messages, processed=%d inserted=%d\n", e.env, e.qtype, len(data), max, e.inserted)
			if len(data) == 0 {
				e.processed = e.inserted
			} else {
				e.processed = max
				for _, d := range data {
					// Don't add inflight messages to the queue. This can happen on a restart.
					// Is there some better way to handle this?
					if _, ok := e.inflights[d.ID]; !ok {
						e.queue = append(e.queue, d)
					}
				}
			}
		}
	}

	if len(e.queue) == 0 {
		return nil, nil
	}

	msg := &inflightEgress{
		Egress: e.queue[0],
		expiry: time.Now().Add(time.Second * inflightTime),
	}
	e.queue = e.queue[1:]

	e.inflights[msg.ID] = msg
	return msg.Egress, nil
}

func (e *envState) init() error {
	messages, err := egress.Inflight(e.env, e.qtype)
	if err != nil {
		return err
	}

	expiry := time.Now().Add(time.Second * inflightTime)
	for _, d := range messages {
		e.inflights[d.ID] = &inflightEgress{
			Egress: d,
			expiry: expiry,
		}
	}
	log.Printf("%d:%d: added %d messages to inflight\n", e.env, e.qtype, len(e.inflights))

	return nil
}

type queueState struct {
	mutex sync.Mutex

	// Round robin list for fair fetching.
	order *list.List

	// Map of env id to env queues for this queue.
	envs map[int]*envState
}

var (
	queues [db.EGRESS_QUEUE_MAX]*queueState

	tokenErr   = errors.New("invalid token")
	missingErr = errors.New("missing data")
)

func Init() error {
	for i := range queues {
		queues[i] = &queueState{
			order: list.New(),
			envs:  map[int]*envState{},
		}
	}

	queues, err := egress.AllQueuesForShard()
	if err != nil {
		return err
	}

	for _, q := range queues {
		if err := updateQueueData(q).init(); err != nil {
			return err
		}
	}
	return nil
}

func updateQueueData(q egress.Queue) *envState {
	state := queues[q.Queue()]

	state.mutex.Lock()
	defer state.mutex.Unlock()

	var e *envState
	if e = state.envs[q.Env()]; e == nil {
		e = &envState{
			env:       q.Env(),
			qtype:     q.Queue(),
			inserted:  q.Inserted(),
			processed: q.Processed(),
			inflights: map[int]*inflightEgress{},
		}
		state.envs[q.Env()] = e
		state.order.PushBack(e)
	} else {
		e.inserted = q.Inserted()
	}
	return e
}

// Refresh is called periodically to update the number of available
// messages in each queue/env.
func Refresh() {
	queues, err := egress.QueuesForShard()
	if err != nil {
		return
	}

	for _, q := range queues {
		if q.Count() == 0 {
			continue
		}

		updateQueueData(q)
	}
}

func min(f map[int]*inflightEgress) int {
	m := 0
	i := 0
	for id := range f {
		if i == 0 || id < m {
			m = id
		}
		i++
	}
	return m
}

func inflight(envID int, qtype db.EgressQueue, id int, token string, fn func(f *inflightEgress, min int) (bool, error)) error {
	state := queues[qtype]

	state.mutex.Lock()
	defer state.mutex.Unlock()

	if env := state.envs[envID]; env != nil {
		var flight *inflightEgress
		if flight = env.inflights[id]; flight == nil {
			return missingErr
		} else if flight.Token != token {
			return tokenErr
		}

		if del, err := fn(flight, min(env.inflights)); err != nil {
			return err
		} else if del {
			delete(env.inflights, id)
		}
		return nil
	}
	return missingErr
}

func pull(queue db.EgressQueue) (int, *egress.Egress, error) {
	state := queues[queue]

	state.mutex.Lock()
	defer state.mutex.Unlock()

	for i := 0; i < state.order.Len(); i++ {
		next := state.order.Front()
		state.order.MoveToBack(next)

		env := next.Value.(*envState)
		if msg, err := env.pull(); err != nil {
			return 0, nil, err
		} else if msg != nil {
			return env.env, msg, nil
		}
	}
	return 0, nil, nil
}

func Fetch(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	var (
		name  = p.ByName("queue")
		q     = egress.NameToQueue(name)
		msg   *egress.Egress
		envID int
		err   error
	)

	if envID, msg, err = pull(q); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, fmt.Sprintf("%v", err))
		return
	} else if msg == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	response := data.Job{
		ID:    msg.ID,
		Token: msg.Token,
		Data:  msg.Data,
		Env:   envID,
		Queue: name,
	}
	log.Printf("fetch: queue=%s env=%d: id=%d token=%s data=%s", name, envID, msg.ID, msg.Token, string(msg.Data))

	body, err := json.Marshal(response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, fmt.Sprintf("%v", err))
		return
	}

	w.Header().Add("Content-Type", "application/json; charset=utf-8")

	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Add("Content-Encoding", "gzip")

		var b bytes.Buffer
		gz := gzip.NewWriter(&b)
		_, _ = gz.Write(body)
		_ = gz.Close()

		body = b.Bytes()
	}

	w.Header().Add("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)

	if _, err := w.Write(body); err != nil {
		log.Println("write", err)
	}
}

func unmarshal(r *http.Request, d interface{}) error {
	reader := io.LimitReader(r.Body, 10*1024)
	if b, err := ioutil.ReadAll(reader); err != nil {
		return err
	} else {
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber()
		if err := dec.Decode(d); err != nil {
			return err
		}
	}
	return nil
}

func Ack(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	var req data.Request
	if err := unmarshal(r, &req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, fmt.Sprintf("%v", err))
		return
	}

	q := egress.NameToQueue(req.Queue)
	if err := inflight(req.Env, q, req.ID, req.Token,
		func(msg *inflightEgress, processed int) (bool, error) {
			if err := egress.Delete(req.Env, req.ID, q, processed); err != nil {
				return false, err
			}
			return true, nil
		}); err != nil {
		code := http.StatusInternalServerError
		if err == tokenErr || err == missingErr {
			code = http.StatusNoContent
		}
		log.Printf("ack: env=%d id=%d tok=%s q=%s: %v", req.Env, req.ID, req.Token, req.Queue, err)
		w.WriteHeader(code)
		fmt.Fprintf(w, fmt.Sprintf("%v", err))
	} else {
		log.Printf("ack: env=%d id=%d tok=%s q=%s: ok", req.Env, req.ID, req.Token, req.Queue)
		w.WriteHeader(http.StatusOK)
	}
}

func Ping(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	var req data.Request
	if err := unmarshal(r, &req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, fmt.Sprintf("%v", err))
		return
	}

	q := egress.NameToQueue(req.Queue)
	if err := inflight(req.Env, q, req.ID, req.Token,
		func(msg *inflightEgress, processed int) (bool, error) {
			msg.expiry = time.Now().Add(time.Second * inflightTime)
			return false, nil
		}); err != nil {
		code := http.StatusInternalServerError
		if err == tokenErr || err == missingErr {
			code = http.StatusNoContent
		}
		log.Printf("ping: env=%d id=%d tok=%s q=%s: %v", req.Env, req.ID, req.Token, req.Queue, err)
		w.WriteHeader(code)
		fmt.Fprintf(w, fmt.Sprintf("%v", err))
	} else {
		log.Printf("ping: env=%d id=%d tok=%s: ok", req.Env, req.ID, req.Token)
		w.WriteHeader(http.StatusOK)
	}
}
