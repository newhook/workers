package workers

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	simplejson "github.com/bitly/go-simplejson"
	"github.com/newhook/workers/fair"
	"github.com/newhook/workers/sql"
)

func makeConcurrencyPool(concurency int) chan func() {
	workChan := make(chan func())
	for i := 0; i < concurency; i++ {
		go func() {
			for work := range workChan {
				work()
			}
		}()
	}
	return workChan
}

var concurrencyPool chan func()

// I think the database performance in this function will suck. What will
// probably happen is all threads will rush in here try to grab ownership of
// the first record causing all of them to deadlock.
//
func limitTo(s string, l int) string {
	if len(s) > l {
		return s[:l]
	}
	return s
}

func fetch(env int, queue string) (*Message, error) {
	// I think the database performance in this function will suck. What will
	// probably happen is all threads will rush in here try to grab ownership of
	// the first record causing all of them to deadlock.
	//
	// Not sure on the ideal solution here. Stored procedures?
	job, ok, err := sql.ClaimJob(env, queue)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	data, err := simplejson.NewJson(job.Data)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	jid := fmt.Sprintf("JID-%d-%d", env, job.ID)
	message := &Message{
		JID:    jid,
		Env:    env,
		Queue:  queue,
		Ctx:    ctx,
		Json:   data,
		raw:    &job,
		cancel: cancel,
	}
	return message, nil
}

func refreshJob(message *Message) chan interface{} {
	done := make(chan interface{})

	go func() {
		defer message.cancel()
	loop:
		for {
			select {
			case r := <-done:
				job := message.raw
				if r == nil || !retry(message.raw) {
					break loop
				}

				job.Error.String = limitTo(fmt.Sprintf("%v", r), 191)
				job.Error.Valid = true

				waitDuration := secondsToDelay(incrementRetry(job))
				job.RetryAt.Int64 = time.Now().Unix() + int64(waitDuration)
				job.RetryAt.Valid = true
				// Clear the inflight flag.
				job.InFlight.Valid = false

				if ok, err := sql.RetryJob(message.Env, *job); err != nil {
					log.Println(message.JID, "retry failed", err)
				} else if !ok {
					log.Println(message.JID, "retry failed because job is no longer owned by this worker")
				}
				return

			case <-time.After(time.Duration(sql.InflightLimit/2) * time.Second):
				// If the refresh failed we had a database error, or the job was stolen
				// by some other worker.
				if ok, err := sql.RefreshJob(message.Env, *message.raw); err != nil {
					log.Println(message.JID, "refresh failed", err)
					message.cancel()
					<-done
					return
				} else if !ok {
					log.Println(message.JID, "refresh failed because job is no longer owned by this worker")
					message.cancel()
					<-done
					return
				}
			}
		}

		if ok, err := sql.DeleteJob(message.Env, *message.raw); err != nil {
			log.Println(message.JID, "delete failed", err)
		} else if !ok {
			log.Println(message.JID, "retry failed because job is no longer owned by this worker")
		}
	}()

	return done
}

func work(w fair.Work) (bool, error) {
	d := w.Data.(*sql.Worker)
	env := d.ID
	queue := d.Queue

	type workStatus struct {
		more bool
		err  error
	}
	ready := make(chan workStatus)

	concurrencyPool <- func() {
		// I think the database performance in this function will suck. What will
		// probably happen is all threads will rush in here try to grab ownership of
		// the first record causing all of them to deadlock.
		//
		// Not sure on the ideal solution here. Stored procedures?
		message, err := fetch(env, queue)

		// Release the fair worker thread immediately after we've fetched the
		// message (or failed).
		ready <- workStatus{message != nil, err}

		// No message, no work to do.
		if message == nil {
			return
		}

		// Refresh the inflight status at regular intervals.
		done := refreshJob(message)

		defer func() {
			if r := recover(); r != nil {
				done <- r
				return
			}
			done <- nil
		}()
		queues[queue](message)
	}

	status := <-ready
	if status.err != nil {
		return false, status.err
	}

	if status.more {
		d.Count--
		return d.Count > 0, nil
	}
	return false, nil
}

func pull() []fair.Work {
	workers, err := sql.FindReady()
	if err != nil {
		log.Println("FindReady", err)
		return nil
	}
	ids := make([]fair.Work, len(workers))
	for i, w := range workers {
		ids[i] = fair.Work{
			ID:   strconv.Itoa(w.ID) + ":" + w.Queue,
			Data: &w,
		}
	}
	return ids
}
