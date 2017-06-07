package serve

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"encoding/json"

	"github.com/julienschmidt/httprouter"
	"github.com/newhook/workers/db"
	"github.com/newhook/workers/httpworkers/data"
)

type env struct {
	id    int
	count int
}
type queue struct {
	order *list.List
	envs  map[int]*list.Element
}

var (
	mutex  sync.Mutex
	queues = map[string]*queue{}
)

func Refresh() {
	workers, err := db.FindWorkers()
	if err != nil {
		return
	}

	mutex.Lock()
	defer mutex.Unlock()
	for _, w := range workers {
		var (
			q  *queue
			ok bool
		)

		if q, ok = queues[w.Queue]; !ok {
			q = &queue{
				order: list.New(),
				envs:  map[int]*list.Element{},
			}
			queues[w.Queue] = q
		}

		if w.Count > 0 {
			if e := q.envs[w.ID]; e == nil {
				q.envs[w.ID] = q.order.PushBack(&env{
					id:    w.ID,
					count: w.Count,
				})
			} else {
				env := e.Value.(*env)
				env.count = w.Count
			}
		} else {
			if e := q.envs[w.ID]; e != nil {
				q.order.Remove(e)
				delete(q.envs, w.ID)
			}
		}
	}
}

func nextEnv(queue string) (int, bool) {
	mutex.Lock()
	defer mutex.Unlock()
	if q, ok := queues[queue]; ok {
		if q.order.Len() == 0 {
			return 0, false
		}
		next := q.order.Front()
		env := next.Value.(*env)
		env.count--
		if env.count > 0 {
			q.order.MoveToBack(next)
		} else {
			q.order.Remove(next)
			delete(q.envs, env.id)
		}
		return env.id, true
	}
	return 0, false
}

func Fetch(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	queue := p.ByName("queue")
	envID, ok := nextEnv(queue)
	if !ok {
		//log.Printf("fetch: queue=%s: no content", queue)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	job, ok, err := db.ClaimJob(envID, queue)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, fmt.Sprintf("%v", err))
		return
	}
	if !ok {
		//log.Printf("fetch: queue=%s env=%d: no content", queue, envID)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	response := data.Job{
		ID:    job.ID,
		Env:   envID,
		Token: job.InFlightTok.String,
		Queue: queue,
		Data:  job.Data,
	}
	log.Printf("fetch: queue=%s env=%d: id=%d token=%s queue=%s data=%s", queue, envID, job.ID, job.InFlightTok.String, queue, string(job.Data))

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

	if ok, err := db.DeleteJobID(req.Env, req.ID, req.Token, req.Queue); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, fmt.Sprintf("%v", err))
		return
	} else if !ok {
		log.Printf("ack: env=%d id=%d tok=%s q=%s: invalid token", req.Env, req.ID, req.Token, req.Queue)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	log.Printf("ack: env=%d id=%d tok=%s q=%s: ok", req.Env, req.ID, req.Token, req.Queue)
	w.WriteHeader(http.StatusOK)
}

func Ping(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	var req data.Request
	if err := unmarshal(r, &req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, fmt.Sprintf("%v", err))
		return
	}

	if ok, err := db.RefreshJobID(req.Env, req.ID, req.Token); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, fmt.Sprintf("%v", err))
		return
	} else if !ok {
		log.Printf("ping: env=%d id=%d tok=%s: invalid token", req.Env, req.ID, req.Token)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	log.Printf("ping: env=%d id=%d tok=%s: ok", req.Env, req.ID, req.Token)
	w.WriteHeader(http.StatusOK)
}
