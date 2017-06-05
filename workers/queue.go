package workers

import (
	"encoding/json"

	simplejson "github.com/bitly/go-simplejson"
	"github.com/newhook/workers/sql"
)

func Queue(env int, queue string, msg interface{}) (int, error) {
	if b, err := json.Marshal(msg); err != nil {
		return 0, err
	} else {
		if j, err := sql.QueueRaw(env, queue, b); err != nil {
			return 0, err
		} else {
			return j.ID, nil
		}
	}
}

func QueueJson(env int, queue string, msg *simplejson.Json) (int, error) {
	if b, err := msg.Encode(); err != nil {
		return 0, err
	} else {
		if j, err := sql.QueueRaw(env, queue, b); err != nil {
			return 0, err
		} else {
			return j.ID, nil
		}
	}
}