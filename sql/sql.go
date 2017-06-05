package sql

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/newhook/workers/db"

	"github.com/jmoiron/sqlx"
)

type Job struct {
	ID         int            `db:"id"`
	Queue      string         `db:"queue"`
	Data       []byte         `db:"data"`
	InFlight   sql.NullInt64  `db:"inflight"`
	Retry      bool           `db:"retry"`
	RetryMax   int            `db:"retry_max"`
	Error      sql.NullString `db:"error"`
	RetryCount sql.NullInt64  `db:"retry_count"`
	RetryAt    sql.NullInt64  `db:"retry_at"`
	FailedAt   sql.NullInt64  `db:"failed_at"`
	RetriedAt  sql.NullInt64  `db:"retried_at"`
	CreatedAt  int            `db:"created_at"`
}

type Worker struct {
	ID    int    `db:"id"`
	Queue string `db:"queue"`
	Count int    `db:"count"`
}

const (
	InflightLimit = 30
)

var (
	readyQuery string
	readyArgs  []interface{}
)

func PrepareQueues(queues []string) error {
	var err error
	readyQuery, readyArgs, err = sqlx.In(`SELECT * FROM `+GlobalName+`.workers WHERE queue IN (?) AND count > 0`, queues)
	if err != nil {
		return err
	}
	return nil
}

func ProcessRetries(queues []string) error {
	now := time.Now().Unix()
	query, args, err := sqlx.In(`SELECT id, queue FROM `+GlobalName+`.retries WHERE queue IN (?) AND retry_at < ?`, queues, now)
	if err != nil {
		return err
	}

	if err := db.Transact(func(tr db.Transactor) error {
		type retry struct {
			ID    int    `db:"id"`
			Queue string `db:"queue"`
		}

		var retries []retry
		query = tr.Rebind(query)
		if err := tr.Select(&retries, query, args...); err != nil {
			return err
		}

		var retriesDatas []interface{}
		var workersDatas []interface{}
		for _, r := range retries {
			fmt.Println("clearing retries for", r.ID, "/", r.Queue)
			if result, err := tr.Exec(`UPDATE `+DatabaseName(r.ID)+`.jobs
                SET retry_at = NULL
				WHERE queue = ? AND retry_at < ?`, r.Queue, now); err != nil {
				return err
			} else {
				n, _ := result.RowsAffected()

				workersDatas = append(workersDatas, r.ID)
				workersDatas = append(workersDatas, r.Queue)
				workersDatas = append(workersDatas, n)
			}

			var retry sql.NullInt64
			retriesDatas = append(retriesDatas, r.ID)
			retriesDatas = append(retriesDatas, r.Queue)

			if err := tr.Get(&retry, `SELECT retry_at FROM `+DatabaseName(r.ID)+`.jobs
				WHERE retry_at IS NOT NULL LIMIT 1`); err != nil && err != sql.ErrNoRows {
				return err
			}

			retriesDatas = append(retriesDatas, retry)
		}

		if len(retriesDatas) > 0 {
			if _, err := tr.Exec(`INSERT INTO `+GlobalName+`.retries
		    (id, queue, retry_at)
				VALUES
			(?,?,?)`+strings.Repeat(",(?,?,?)", (len(retriesDatas)/3)-1)+`
			ON DUPLICATE KEY UPDATE retry_at=VALUES(retry_at)`, retriesDatas...); err != nil {
				return err
			}
		}

		if len(workersDatas) > 0 {
			if _, err := tr.Exec(`INSERT INTO `+GlobalName+`.workers
		    (id, queue, count)
				VALUES
			(?,?,?)`+strings.Repeat(",(?,?,?)", (len(workersDatas)/3)-1)+`
			ON DUPLICATE KEY UPDATE count=count+VALUES(count)`, workersDatas...); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func FindReady() ([]Worker, error) {
	tr := db.DB()
	var workers []Worker
	if err := tr.Select(&workers, tr.Rebind(readyQuery), readyArgs...); err != nil {
		return nil, err
	}
	return workers, nil
}

func FindReadyRaw(queues []string) ([]Worker, error) {
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

func Queue(env int, queue string, data []byte) (Job, error) {
	now := time.Now().Unix()
	j := Job{
		Queue:     queue,
		Data:      data,
		Retry:     true,
		RetryMax:  30,
		CreatedAt: int(now),
	}

	if err := db.Transact(func(tr db.Transactor) error {
		if result, err := tr.NamedExec(`INSERT INTO `+DatabaseName(env)+`.jobs
			(queue, data, retry, retry_max, created_at)
		VALUES
			(:queue, :data, :retry, :retry_max, :created_at)`, &j); err != nil {
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

	now := time.Now().Unix()
	inflightLimit := now + InflightLimit

	// Stored procedure?
	if err := db.Transact(func(tr db.Transactor) error {
		if err := tr.Get(&j, `SELECT * FROM `+DatabaseName(env)+`.jobs WHERE queue = ? AND (inflight IS NULL or inflight < ?) AND retry_at IS NULL LIMIT 1 FOR UPDATE`, queue, now); err != nil {
			return err
		}
		if _, err := tr.Exec(`UPDATE `+DatabaseName(env)+`.jobs SET inflight = ? WHERE id = ?`, inflightLimit, j.ID); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return Job{}, err
	}
	return j, nil
}

func RetryJob(env int, j Job) error {
	if err := db.Transact(func(tr db.Transactor) error {
		if _, err := tr.NamedExec(
			`UPDATE `+DatabaseName(env)+`.jobs SET
				inflight = :inflight,
				retry = :retry,
				retry_max = :retry_max,
				error = :error,
				retry_count = :retry_count,
				retry_at = :retry_at,
				failed_at = :failed_at,
				retried_at = :retried_at
				WHERE id = :id`, j); err != nil {
			return err
		}
		if _, err := tr.Exec(
			`INSERT INTO `+GlobalName+`.retries
			   (id, queue, retry_at)
             VALUES
			   (?, ?, ?)
             ON DUPLICATE KEY UPDATE retry_at=IF(retry_at < VALUES(retry_at),retry_at,VALUES(retry_at))`, env, j.Queue, j.RetryAt.Int64); err != nil {
			return err
		}

		if _, err := tr.Exec(
			`UPDATE `+GlobalName+`.workers
			 SET count = count - 1
		     WHERE id = ? AND queue = ?`, env, j.Queue); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}
	return nil
}

func DeleteJob(env int, j Job) error {
	if err := db.Transact(func(tr db.Transactor) error {
		if _, err := tr.Exec(`DELETE FROM `+DatabaseName(env)+`.jobs WHERE id = ?`, j.ID); err != nil {
			return err
		}

		if _, err := tr.Exec(
			`UPDATE `+GlobalName+`.workers
			 SET count = count - 1
		     WHERE id = ? AND queue = ?`, env, j.Queue); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}
	return nil
}

func RefreshJob(env int, j Job) error {
	tr := db.DB()
	if _, err := tr.Exec(`UPDATE `+DatabaseName(env)+`.jobs SET inflight = ? WHERE id = ?`, time.Now().Unix()+InflightLimit, j.ID); err != nil {
		return err
	}
	return nil
}
