package workers

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	simplejson "github.com/bitly/go-simplejson"
	"github.com/newhook/workers/sql"
)

func limitTo(s string, l int) string {
	if len(s) > l {
		return s[:l]
	}
	return s
}

func dowork(env int, queue string) error {
	// I think the database performance in this function will suck. What will
	// probably happen is all threads will rush in here try to grab ownership of
	// the first record causing all of them to deadlock.
	//
	// Not sure on the ideal solution here. Stored procedures?
	job, err := sql.ClaimJob(env, queue)
	if err != nil {
		return err
	}

	msg, err := simplejson.NewJson(job.Data)
	if err != nil {
		return err
	}

	done := make(chan interface{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("recovering", r)
				done <- r
				return
			}
			done <- nil
		}()
		jid := fmt.Sprintf("JID-%d-%d", env, job.ID)
		queues[queue](&Message{jid, env, queue, msg, &job})
	}()

loop:
	for {
		select {
		case r := <-done:
			if r == nil {
				break loop
			}
			if retry(job) {
				job.Error.String = limitTo(fmt.Sprintf("%v", r), 191)
				job.Error.Valid = true

				waitDuration := secondsToDelay(incrementRetry(&job))
				job.RetryAt.Int64 = time.Now().Unix() + int64(waitDuration)
				job.RetryAt.Valid = true
				// Clear the inflight flag.
				job.InFlight.Valid = false

				if err := sql.RetryJob(env, job); err != nil {
					return err
				} else {
					return nil
				}
			}
			break loop

		case <-time.After(10 * time.Second):
			if err := sql.RefreshJob(env, job); err != nil {
				return err
			}
		}
	}

	return sql.DeleteJob(env, job)
}

const (
	DEFAULT_MAX_RETRY   = 25
	NanoSecondPrecision = 1000000000.0
)

func durationToSecondsWithNanoPrecision(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / NanoSecondPrecision
}

func retry(message sql.Job) bool {
	max := DEFAULT_MAX_RETRY
	retry := message.Retry
	count := int(message.RetryCount.Int64)
	if message.RetryMax != 0 {
		max = message.RetryMax
	}
	return retry && count < max
}

func incrementRetry(message *sql.Job) int {
	if !message.RetryCount.Valid {
		message.FailedAt.Int64 = time.Now().Unix()
		message.FailedAt.Valid = true
	} else {
		message.RetriedAt.Int64 = time.Now().Unix()
		message.RetriedAt.Valid = true
	}
	message.RetryCount.Int64++
	message.RetryCount.Valid = true

	return int(message.RetryCount.Int64)
}

func secondsToDelay(count int) int {
	power := math.Pow(float64(count), 4)
	return int(power) + 15 + (rand.Intn(30) * (count + 1))
}
