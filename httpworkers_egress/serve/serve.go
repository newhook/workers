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
	"github.com/pborman/uuid"
)

type flight struct {
	*egress.Egress
	id     int
	token  string
	env    int
	queue  db.EgressQueue
	expiry time.Time
}

type envQueue struct {
	mutex sync.Mutex

	env   int            // The env id.
	queue db.EgressQueue // The queue

	// Highest entry in the database.
	inserted int

	// Next id to fetch.
	processed int

	inflight map[int]*flight // Set of inflight messages for this env/queue.

	e *list.Element // Entry in the RR set for this queue.
}

type queue struct {
	mutex sync.Mutex
	// Round robin list for fair fetching.
	order *list.List
	// Map of env id to env queues for this queue.
	envs map[int]*envQueue
}

var (
	queues [db.EGRESS_QUEUE_MAX]*queue

	invalidTokenErr = errors.New("invalid token")
	noDataErr       = errors.New("missing data")
)

func Refresh() {
	eq, err := egress.QueuesForShard()
	if err != nil {
		return
	}

	for _, w := range eq {
		q := queues[w.Queue()]
		if w.Count() == 0 {
			continue
		}
		q.mutex.Lock()
		if e := q.envs[w.Env()]; e == nil {
			fmt.Println("add", w)
			e := &envQueue{
				env:       w.Env(),
				queue:     w.Queue(),
				inserted:  w.Inserted(),
				processed: w.Processed(),
				inflight:  map[int]*flight{},
			}
			q.envs[w.Env()] = e
			e.e = q.order.PushBack(e)
		} else {
			fmt.Printf("%p: updated inserted %d\n", e, e.inserted)
			e.inserted = w.Inserted()
			if e.e == nil {
				e.e = q.order.PushBack(e)
			}
		}
		q.mutex.Unlock()
	}
}

func min(f map[int]*flight) int {
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

func inflight(envID int, qname string, id int, token string, fn func(f *flight, min int) (bool, error)) error {
	q := queues[egress.NameToQueue(qname)]
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if env := q.envs[envID]; env != nil {
		env.mutex.Lock()
		defer env.mutex.Unlock()

		flight := env.inflight[id]
		if flight == nil {
			return noDataErr
		}

		if flight.token != token {
			return invalidTokenErr
		}

		if del, err := fn(flight, min(env.inflight)); err != nil {
			return err
		} else if del {
			delete(env.inflight, id)
		}
		return nil
	}
	return noDataErr
}

func init() {
	for i := range queues {
		queues[i] = &queue{
			order: list.New(),
			envs:  map[int]*envQueue{},
		}
	}
}

func (e *envQueue) pull() (*flight, error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	now := time.Now()
	for _, flight := range e.inflight {
		if now.After(flight.expiry) {
			log.Println("expiring inflight message", flight.ID)
			flight.expiry = time.Now().Add(time.Second * 60)
			flight.token = uuid.New()
			return flight, nil
		}
	}

	// TODO: Can pulln messages and put them in a cache.
	//log.Println("pulln", e.env, e.queue, e.processed)
	fmt.Printf("%p: e.processed %d\n", e, e.processed)
	data, max, err := egress.Pulln(e.env, e.queue, e.processed, 1)
	//log.Println("pulln", data, max, err)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	e.processed = max
	fmt.Printf("%p: e.processed: %d\n", e, max)

	flight := &flight{
		Egress: data[0],
		env:    e.env,
		token:  uuid.New(),
		queue:  e.queue,
		expiry: time.Now().Add(time.Second * 60),
	}
	e.inflight[data[0].ID] = flight
	return flight, nil
}

func nextEnvQueue(queue db.EgressQueue) *envQueue {
	q := queues[queue]
	q.mutex.Lock()
	defer q.mutex.Unlock()
	for {
		if q.order.Len() == 0 {
			return nil
		}
		next := q.order.Front()
		env := next.Value.(*envQueue)
		if env.processed < env.inserted {
			q.order.MoveToBack(next)
			return env
		} else {
			q.order.Remove(next)
			env.e = nil
		}
	}
	return nil
}

func Fetch(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	name := p.ByName("queue")
	queue := egress.NameToQueue(name)
	env := nextEnvQueue(queue)
	if env == nil {
		//log.Printf("fetch: queue=%s: no content", queue)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	flight, err := env.pull()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, fmt.Sprintf("%v", err))
		return
	}
	// Nothing to pull for this env and queue.
	if flight == nil {
		//log.Printf("fetch: queue=%s env=%d: no content", queue, envID)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	response := data.Job{
		ID:    flight.ID,
		Env:   env.env,
		Token: flight.token,
		Queue: name,
		Data:  flight.Data,
	}
	log.Printf("fetch: queue=%s env=%d: id=%d token=%s data=%s", name, env.env, flight.ID, flight.token, string(flight.Data))

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

	if err := inflight(req.Env, req.Queue, req.ID, req.Token,
		func(flight *flight, processed int) (bool, error) {
			if err := egress.Delete(flight.env, flight.id, flight.queue, processed); err != nil {
				return false, err
			}
			return true, nil
		}); err != nil {
		if err == invalidTokenErr {
			log.Printf("ack: env=%d id=%d tok=%s q=%s: invalid token", req.Env, req.ID, req.Token, req.Queue)
			w.WriteHeader(http.StatusNoContent)
		} else if err == noDataErr {
			log.Printf("ack: env=%d id=%d tok=%s q=%s: no job", req.Env, req.ID, req.Token, req.Queue)
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, fmt.Sprintf("%v", err))
		}
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

	if err := inflight(req.Env, req.Queue, req.ID, req.Token,
		func(flight *flight, processed int) (bool, error) {
			flight.expiry = time.Now().Add(time.Second * 60)
			return false, nil
		}); err != nil {
		if err == invalidTokenErr {
			log.Printf("ping: env=%d id=%d tok=%s q=%s: invalid token", req.Env, req.ID, req.Token, req.Queue)
			w.WriteHeader(http.StatusNoContent)
		} else if err == noDataErr {
			log.Printf("ping: env=%d id=%d tok=%s q=%s: no job", req.Env, req.ID, req.Token, req.Queue)
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, fmt.Sprintf("%v", err))
		}
	} else {
		log.Printf("ping: env=%d id=%d tok=%s: ok", req.Env, req.ID, req.Token)
		w.WriteHeader(http.StatusOK)
	}
}
