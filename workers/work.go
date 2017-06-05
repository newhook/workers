package workers

import (
	"time"

	simplejson "github.com/bitly/go-simplejson"
	"github.com/newhook/workers/sql"
)

func dowork(env int, queue string) error {
	job, err := sql.ClaimJob(env, queue)
	if err != nil {
		return err
	}

	msg, err := simplejson.NewJson(job.Data)
	if err != nil {
		return err
	}

	done := make(chan struct{})
	go func() {
		defer func() {
			close(done)
		}()
		queues[queue](&Message{msg})
	}()

loop:
	for {
		select {
		case _ = <-done:
			break loop

		case <-time.After(10 * time.Second):
			if err := sql.RefreshJob(job); err != nil {
				return err
			}
		}
	}

	return sql.DeleteJob(job)
}
