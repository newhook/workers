package egress

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	simplejson "github.com/bitly/go-simplejson"
	"github.com/newhook/workers/core"
	"github.com/newhook/workers/db"
)

func pushtx(envID int, t db.Transactor, queue db.EgressQueue, data []byte) error {
	var egress []interface{}
	var egressQ [db.EGRESS_QUEUE_MAX]bool

	egressQ[queue] = true
	egress = append(egress, queue)
	egress = append(egress, data)

	return flush(envID, t, egressQ, egress)
}

func flush(envID int, t db.Transactor, queues [db.EGRESS_QUEUE_MAX]bool, datas []interface{}) error {
	sharddb := db.GlobalName
	dbname := db.DatabaseName(envID)

	_, err := t.Exec(`insert into `+dbname+`.egress
	  (queue, data)
	  VALUES
	  (?,?)`+strings.Repeat(",(?,?)", len(datas)/2-1), datas...)
	if err != nil {
		return err
	}

	datas = datas[:0]
	for q, v := range queues {
		if v {
			datas = append(datas, envID)
			datas = append(datas, q)
			datas = append(datas, q)
		}
	}

	s := `(?,?,(SELECT MAX(E.id) FROM ` + dbname + `.egress E WHERE queue = ?))`

	_, err = t.Exec(`INSERT INTO `+sharddb+`.egress
		  (id, queue, inserted)
		VALUES `+
		s+strings.Repeat(","+s, len(datas)/3-1)+`
		ON DUPLICATE KEY UPDATE queue=VALUES(queue), inserted=VALUES(inserted);`, datas...)

	return err
}

// DefaultFacet is a default implementation to determine
// the facet from the message content.
func DefaultFacet(message *simplejson.Json) string {
	return strconv.Itoa(message.Get("environment_id").MustInt())
}

// DefaultChannel is a default implementation to determine
// the channel from the message content.
func DefaultChannel(message *simplejson.Json) string {
	envId := message.Get("environment_id").MustInt()
	messageType := message.Get("type").MustString()

	channel := "env:" + strconv.Itoa(envId) + ":type:" + messageType

	if messageType == "event" {
		eventType := message.GetPath("data", "type").MustString()
		channel = channel + ":" + eventType
	}

	return channel
}

var (
	// Channel is the function used to determine the channel for a message.
	Channel = DefaultChannel
	// Facet is the function used to determine the facet for a message.
	Facet = DefaultFacet

	typePrefix = ":type:"
)

// Map the fairway channel to an egress channel. See DefaultChannel above.
// env:<id>:type:<message-type>
func ChannelToQueue(channel string) db.EgressQueue {
	if i := strings.Index(channel, ":type:"); i != -1 {
		messageType := channel[i+len(typePrefix):]
		switch messageType {
		case "webhook":
			return db.EGRESS_QUEUE_WEBHOOK
		case "delivery_event":
			return db.EGRESS_QUEUE_DELIVERY_EVENT
		case "send_priority_delivery":
			return db.EGRESS_QUEUE_SEND_PRIORITY_DELIVERY
		case "send_delivery":
			return db.EGRESS_QUEUE_SEND_DELIVERY
		case "attribute_render":
			return db.EGRESS_QUEUE_ATTRIBUTE_RENDER
		}
	}
	// Log on this case since this is unexpected.

	// I don't think we have any events any longer.
	// strings.HasPrefix(messageType, "event") {
	log.Printf("unknown egress message type", channel)
	return db.EGRESS_QUEUE_DEFAULT
}

func NameToQueue(name string) db.EgressQueue {
	switch name {
	case "webhook":
		return db.EGRESS_QUEUE_WEBHOOK
	case "delivery_event":
		return db.EGRESS_QUEUE_DELIVERY_EVENT
	case "send_priority_delivery":
		return db.EGRESS_QUEUE_SEND_PRIORITY_DELIVERY
	case "send_delivery":
		return db.EGRESS_QUEUE_SEND_DELIVERY
	case "attribute_render":
		return db.EGRESS_QUEUE_ATTRIBUTE_RENDER
	}
	return db.EGRESS_QUEUE_DEFAULT
}

func QueueMsgTx(envID int, tx db.Transactor, msg map[string]interface{}) error {
	// Add a message uuid.
	//if !config.IsTestEnv() {
	//msg["_uuid"] = uuid.New()
	//}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	sj, err := simplejson.NewJson(data)
	if err != nil {
		return err
	}

	facet := Facet(sj)
	channel := Channel(sj)

	return QueueBytesTx(envID, tx, channel, facet, data)
}

func QueueBytesTx(envID int, tx db.Transactor, channel, facet string, data []byte) error {
	e := core.Egress{
		Channel: channel,
		Facet:   facet,
		Data:    data,
	}

	if bytes, err := e.Marshal(); err != nil {
		return err
	} else {
		return pushtx(envID, tx, ChannelToQueue(channel), bytes)
	}
}

type logLogger struct {
}

func (l *logLogger) Log(args ...interface{}) {
	log.Println(args...)
}
func (l *logLogger) Logf(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func deleten(envID int, queue db.EgressQueue, processed int) error {
	//sharddb := db.ShardName()
	//dbname := db.DatabaseName(envID)

	//ctx := ciocontext.NewEnv(context.Background(), envID)
	//rtxr, err := db.ReadTransactorFor(ctx)
	//if err != nil {
	//return err
	//}
	sharddb := db.GlobalName
	dbname := db.DatabaseName(envID)
	rtxr := db.DB()

	if _, err := rtxr.Exec(`UPDATE `+sharddb+`.egress SET processed = ? WHERE queue = ? AND id = ?`, processed, queue, envID); err != nil {
		return err
	}

	if _, err := rtxr.Exec(`DELETE FROM `+dbname+`.egress WHERE queue = ? AND id < ?`, queue, processed); err != nil {
		return err
	}

	return nil
}

func Delete(envID int, id int, queue db.EgressQueue, processed int) error {
	//sharddb := db.ShardName()
	//dbname := db.DatabaseName(envID)

	//ctx := ciocontext.NewEnv(context.Background(), envID)
	//rtxr, err := db.ReadTransactorFor(ctx)
	//if err != nil {
	//return err
	//}
	sharddb := db.GlobalName
	dbname := db.DatabaseName(envID)
	db := db.DB()

	if _, err := db.Exec(`UPDATE `+sharddb+`.egress SET processed = ? WHERE queue = ? AND id = ?`, processed, queue, envID); err != nil {
		return err
	}

	if _, err := db.Exec(`DELETE FROM `+dbname+`.egress WHERE queue = ? AND id = ?`, queue, id); err != nil {
		return err
	}

	return nil
}

type Egress struct {
	ID int
	core.Egress
}

func Pulln(envID int, queue db.EgressQueue, processed, size int) ([]*Egress, int, error) {
	dbname := db.DatabaseName(envID)
	tr := db.DB()

	rows, err := tr.Query(`SELECT id, data FROM `+dbname+`.egress WHERE queue = ? AND id > ? ORDER BY id ASC LIMIT ?;`, queue, processed, size)
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Println("rows.Close failed", err)
		}
	}()

	var messages []*Egress
	var max int
	for rows.Next() {
		var (
			m    Egress
			data []byte
		)
		if err := rows.Scan(&m.ID, &data); err != nil {
			return nil, 0, err
		}
		if err := m.Egress.Unmarshal(data); err != nil {
			return nil, 0, err
		}
		if m.ID > max {
			max = m.ID
		}
		messages = append(messages, &m)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return messages, max, nil
}

// For tests.
/*
func DeliverAll(tx *db.Txn, q Queue) error {
	for {
		messages, max, err := pulln(q.envID, q.processed, 100)
		if err != nil {
			return err
		}

		// If there is nothing left to get we're done. The worker will be woken when more messages are available.
		if len(messages) == 0 {
			return nil
		}

		for _, m := range messages {
			fmt.Println(m.Channel, m.Facet, string(m.Data))
			if err := fairway.DeliverBytes(m.Channel, m.Facet, m.Data); err == nil {
				return err
			}
		}

		if err := deleten(q.envID, max); err != nil {
			return err
		}
	}
}
*/

type Queue struct {
	envID     int
	processed int
	inserted  int
	queue     db.EgressQueue
}

func (q Queue) String() string {
	return fmt.Sprintf("envID=%d queue=%d processed=%d count=%d", q.envID, q.queue, q.Processed(), q.Count())
}

func (q Queue) Key() string {
	return strconv.Itoa(q.envID) + ":" + strconv.Itoa(int(q.queue))
}

func (q Queue) Env() int {
	return q.envID
}

func (q Queue) Queue() db.EgressQueue {
	return q.queue
}

func (q Queue) Count() int {
	return q.inserted - q.processed
}

func (q Queue) Inserted() int {
	return q.inserted
}

func (q Queue) Processed() int {
	return q.processed
}

func QueuesForShard() ([]Queue, error) {
	sharddb := db.GlobalName
	txr := db.DB()

	rows, err := txr.Query(`SELECT id, queue, inserted, processed FROM ` + sharddb + `.egress WHERE inserted > processed`)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Println("rows.Close failed", err)
		}
	}()

	var queues []Queue
	for rows.Next() {
		var q Queue
		if err := rows.Scan(&q.envID, &q.queue, &q.inserted, &q.processed); err != nil {
			return queues, err
		}
		queues = append(queues, q)
	}

	if err := rows.Err(); err != nil {
		return queues, err
	}
	return queues, nil
}
