package sql

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/newhook/workers/db"

	simplejson "github.com/bitly/go-simplejson"
	"github.com/jmoiron/sqlx"
)

type Job struct {
	ID            int           `db:"id"`
	EnvironmentID int           `db:"environment_id"`
	Queue         string        `db:"queue"`
	Data          []byte        `db:"data"`
	InFlight      sql.NullInt64 `db:"in_flight"`
	CreatedAt     int           `db:"created_at"`
	UpdatedAt     int           `db:"updated_at"`
}

type Worker struct {
	ID    int    `db:"id"`
	Queue string `db:"queue"`
	Count int    `db:"count"`
}

func FindReady(queues []string) ([]Worker, error) {
	tr := db.DB()
	// XXX: PREPARE
	query, args, err := sqlx.In(`SELECT * FROM `+GlobalName+`.workers WHERE queue IN (?) AND count > 0`, queues)
	if err != nil {
		return nil, err
	}

	var workers []Worker

	query = tr.Rebind(query)
	if err := tr.Select(&workers, query, args...); err != nil {
		return nil, err
	}
	return workers, nil
}

func Queue(env int, queue string, msg interface{}) (Job, error) {
	if b, err := json.Marshal(msg); err != nil {
		return Job{}, err
	} else {
		return QueueRaw(env, queue, b)
	}
}

func QueueJson(env int, queue string, msg *simplejson.Json) (Job, error) {
	if b, err := msg.Encode(); err != nil {
		return Job{}, err
	} else {
		return QueueRaw(env, queue, b)
	}
}

func QueueRaw(env int, queue string, data []byte) (Job, error) {
	now := time.Now().Unix()
	j := Job{
		EnvironmentID: env,
		Queue:         queue,
		Data:          data,
		CreatedAt:     int(now),
		UpdatedAt:     int(now),
	}

	if err := db.Transact(func(tr db.Transactor) error {
		if result, err := tr.NamedExec(`INSERT INTO `+DatabaseName(env)+`.jobs
			(environment_id, queue, data, created_at, updated_at)
		VALUES
			(:environment_id, :queue, :data, :created_at, :updated_at)`, &j); err != nil {
			return err
		} else {
			id, _ := result.LastInsertId()
			j.ID = int(id)
		}
		if _, err := tr.Exec(
			`INSERT INTO `+GlobalName+`.workers
			   (id, queue, count)
             VALUES
			   (?, ?, 1)
             ON DUPLICATE KEY UPDATE count=count + 1`, env, queue); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return Job{}, err
	}
	return j, nil
}

func ClaimJob(env int, queue string) (Job, error) {
	var j Job
	// XXX: Stored procedure?
	if err := db.Transact(func(tr db.Transactor) error {
		if err := tr.Get(&j, `SELECT * FROM `+DatabaseName(env)+`.jobs WHERE environment_id = ? AND queue = ? AND in_flight is NULL LIMIT 1`, env, queue); err != nil {
			return err
		}
		if _, err := tr.Exec(`UPDATE `+DatabaseName(env)+`.jobs SET in_flight = ? WHERE id = ?`, time.Now().Unix(), j.ID); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return Job{}, err
	}
	return j, nil
}

func DeleteJob(j Job) error {
	if err := db.Transact(func(tr db.Transactor) error {
		if _, err := tr.Exec(`DELETE FROM `+DatabaseName(j.EnvironmentID)+`.jobs WHERE id = ?`, j.ID); err != nil {
			return err
		}

		if _, err := tr.Exec(
			`UPDATE `+GlobalName+`.workers
			 SET count = count - 1
		     WHERE id = ? AND queue = ?`, j.EnvironmentID, j.Queue); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}
	return nil
}

func RefreshJob(j Job) error {
	tr := db.DB()
	if _, err := tr.Exec(`UPDATE `+DatabaseName(j.EnvironmentID)+`.jobs SET in_flight = ? WHERE id = ?`, time.Now(), j.ID); err != nil {
		return err
	}
	return nil
}
