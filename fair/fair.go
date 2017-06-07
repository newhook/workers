package fair

import (
	"fmt"
	"io"
	"log"
	"sync"
	"time"
)

type Work struct {
	ID   string
	Data interface{}
}
type WorkFunc func(work Work) (bool, error)
type PullFunc func() []Work

type Pool struct {
	stop chan struct{}

	// counting semaphore which permits only cap(sem) units of work to run concurrently.
	sem chan struct{}

	// Set of queue workers by name.
	workers map[string]chan Work

	wg sync.WaitGroup

	work WorkFunc
	pull PullFunc
}

func (w *Pool) doWorkUnits(work Work) error {
	for {
		select {
		case <-w.stop:
			return nil

		case <-w.sem:
			more, err := w.work(work)
			w.sem <- struct{}{}
			if err != nil || !more {
				return err
			}
		}
	}
	return nil
}

func newWorker(w *Pool, id string) chan Work {
	fmt.Println("worker for", id)
	w.wg.Add(1)
	wake := make(chan Work)

	go func() {
		defer w.wg.Done()
		for {
			work, ok := <-wake
			if !ok {
				return
			}

			if err := w.doWorkUnits(work); err != nil {
				log.Println("error", err)
			}
		}
	}()

	return wake
}

func (p *Pool) wake(work []Work) {
	for _, w := range work {
		wake, ok := p.workers[w.ID]
		if ok {
			// Wake the worker using a non-blocking send.
			// If the worker isn't listening, it's still processing
			// the last one we sent it, we'll refresh it next time around.
			select {
			case wake <- w:
			default:
			}
		} else {
			// Create a new worker and wake with a blocking send.
			// That ensures that the worker wakes immediately.
			wake = newWorker(p, w.ID)
			p.workers[w.ID] = wake
			wake <- w
		}
	}
}

func New(work WorkFunc, pull PullFunc, writer io.Writer) *Pool {
	w := &Pool{
		stop:    make(chan struct{}),
		sem:     make(chan struct{}, 10),
		workers: map[string]chan Work{},
		work:    work,
		pull:    pull,
	}
	for i := 0; i < cap(w.sem); i++ {
		w.sem <- struct{}{}
	}
	return w
}

func (w *Pool) Run() {
	for {
		w.wake(w.pull())
		select {
		case <-w.stop:
			return
		case <-time.After(time.Second):
		}
	}
}

func (w *Pool) Step() {
	w.wake(w.pull())
}

func (w *Pool) Shutdown() {
	if w.stop != nil {
		close(w.stop)
		for _, wake := range w.workers {
			close(wake)
		}
		w.wg.Wait()
	}
}
