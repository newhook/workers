package workers

import (
	"context"

	simplejson "github.com/bitly/go-simplejson"
	"github.com/newhook/workers/db"
)

type Message struct {
	JID   string
	Env   int
	Queue string
	Ctx   context.Context
	*simplejson.Json
	raw    *db.Job
	cancel context.CancelFunc
}

func (m *Message) Retry(count int) {
	if count > 0 {
		m.raw.RetryMax = count
	} else {
		m.raw.Retry = false
	}
}

func (m *Message) RetryAttempt() int {
	return int(m.raw.RetryCount.Int64)
}
