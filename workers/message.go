package workers

import (
	simplejson "github.com/bitly/go-simplejson"
	"github.com/newhook/workers/sql"
)

type Message struct {
	JID   string
	Env   int
	Queue string
	*simplejson.Json
	raw *sql.Job
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
