package sql

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/newhook/workers/db"
	"github.com/pborman/uuid"

	"github.com/jmoiron/sqlx"
)

type Job struct {
	ID          int            `db:"id"`
	Queue       string         `db:"queue"`
	Data        []byte         `db:"data"`
	InFlight    sql.NullInt64  `db:"inflight"`
	InFlightTok sql.NullString `db:"inflight_tok"`
	Retry       bool           `db:"retry"`
	RetryMax    int            `db:"retry_max"`
	Error       sql.NullString `db:"error"`
	RetryCount  sql.NullInt64  `db:"retry_count"`
	RetryAt     sql.NullInt64  `db:"retry_at"`
	FailedAt    sql.NullInt64  `db:"failed_at"`
	RetriedAt   sql.NullInt64  `db:"retried_at"`
	CreatedAt   int            `db:"created_at"`
}

type Worker struct {
	ID       int    `db:"id"`
	Queue    string `db:"queue"`
	Count    int    `db:"count"`
	Inflight int    `db:"inflight"`
}

var (
	// How long a message is considered in-flight before being picked
	// up for work by a different worker.
	InflightLimit = int64(60)
)

func ProcessRetries() error {
	if err := db.Transact(func(tr db.Transactor) error {
		type retry struct {
			ID    int    `db:"id"`
			Queue string `db:"queue"`
		}

		now := time.Now().Unix()
		var retries []retry
		if err := tr.Select(&retries, `SELECT id, queue FROM `+GlobalName+`.retries WHERE retry_at < ?`, now); err != nil {
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

func ProcessInflight() error {
	if err := db.Transact(func(tr db.Transactor) error {
		var workers []Worker
		if err := tr.Select(&workers, `SELECT * FROM `+GlobalName+`.workers WHERE inflight > 0`); err != nil {
			return err
		}

		var datas []interface{}
		for _, w := range workers {
			now := time.Now().Unix()
			var j []Job
			if err := tr.Select(&j, `SELECT * FROM `+DatabaseName(w.ID)+`.jobs
                WHERE queue = ? AND inflight < ?`, w.Queue, now); err != nil {
				return err
			}
			if len(j) > 0 {
				fmt.Println(j)
			}
			//fmt.Println("clearing inflight for", w.ID, "/", w.Queue)
			if result, err := tr.Exec(`UPDATE `+DatabaseName(w.ID)+`.jobs
			    SET inflight = NULL, inflight_tok = NULL
                WHERE queue = ? AND inflight < ?`, w.Queue, now); err != nil {
				return err
			} else {
				n, _ := result.RowsAffected()
				if n > 0 {
					fmt.Println(w.ID, "/", w.Queue, " cleared", n, "inflight records")
					datas = append(datas, w.ID)
					datas = append(datas, w.Queue)
					datas = append(datas, n)
					datas = append(datas, n)
				}
			}
		}

		if len(datas) > 0 {
			if _, err := tr.Exec(`INSERT INTO `+GlobalName+`.workers
		    (id, queue, count, inflight)
				VALUES
			(?,?,?,?)`+strings.Repeat(",(?,?,?,?)", (len(datas)/4)-1)+`
			ON DUPLICATE KEY UPDATE count=count+VALUES(count),inflight=inflight-VALUES(inflight)`, datas...); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

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

func FindReady() ([]Worker, error) {
	tr := db.DB()
	var workers []Worker
	if err := tr.Select(&workers, tr.Rebind(readyQuery), readyArgs...); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return workers, nil
}

func FindReadyRaw(queues []string) ([]Worker, error) {
	tr := db.DB()
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
func FindReadyQueue(queue string) ([]Worker, error) {
	tr := db.DB()
	var workers []Worker
	if err := tr.Select(&workers, `SELECT * FROM `+GlobalName+`.workers WHERE queue = ? AND count > 0`, queue); err != nil {
		return nil, err
	}
	return workers, nil
}

// TODO: Add support for batch queuing of messages.
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
             ON DUPLICATE KEY UPDATE count=count+VALUES(count)`, env, queue); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return Job{}, err
	}
	return j, nil
}

func ClaimJob(env int, queue string) (Job, bool, error) {
	var (
		j   Job
		ok  bool
		now = time.Now().Unix()
	)

	// Stored procedure?
	if err := db.Transact(func(tr db.Transactor) error {
		if err := tr.Get(&j, `SELECT * FROM `+DatabaseName(env)+`.jobs WHERE queue = ? AND inflight IS NULL AND retry_at IS NULL LIMIT 1 FOR UPDATE`, queue); err != nil {
			if err == sql.ErrNoRows {
				ok = false
				return nil
			}
			return err
		}
		ok = true

		j.InFlightTok = sql.NullString{String: uuid.New(), Valid: true}
		j.InFlight = sql.NullInt64{Int64: int64(now + InflightLimit), Valid: true}
		if _, err := tr.NamedExec(`UPDATE `+DatabaseName(env)+`.jobs SET inflight = :inflight, inflight_tok = :inflight_tok WHERE id = :id`, j); err != nil {
			return err
		}

		if _, err := tr.Exec(
			`INSERT INTO `+GlobalName+`.workers
			   (id, queue, count, inflight)
             VALUES
			   (?, ?, 1, 1)
             ON DUPLICATE KEY UPDATE count=count-VALUES(count),inflight=inflight+VALUES(inflight)`, env, queue); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return Job{}, false, err
	}
	return j, ok, nil
}

func RetryJob(env int, j Job) (bool, error) {
	var ok bool
	if err := db.Transact(func(tr db.Transactor) error {

		// XXX: What happens if this fails because the tok has expired?
		if result, err := tr.NamedExec(
			`UPDATE `+DatabaseName(env)+`.jobs SET
					inflight = NULL,
					inflight_tok = NULL,
					retry = :retry,
					retry_max = :retry_max,
					error = :error,
					retry_count = :retry_count,
					retry_at = :retry_at,
					failed_at = :failed_at,
					retried_at = :retried_at
				WHERE id = :id AND inflight_tok = :inflight_tok`, j); err != nil {
			return err
		} else {
			n, _ := result.RowsAffected()
			if n == 0 {
				return nil
			}
		}
		ok = true

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
			 SET inflight = inflight -1
		     WHERE id = ? AND queue = ?`, env, j.Queue); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return ok, err
	}
	return ok, nil
}

func DeleteJob(env int, j Job) (bool, error) {
	var ok bool
	if err := db.Transact(func(tr db.Transactor) error {
		if result, err := tr.NamedExec(`DELETE FROM `+DatabaseName(env)+`.jobs WHERE id = :id AND inflight_tok = :inflight_tok`, j); err != nil {
			return err
		} else {
			n, _ := result.RowsAffected()
			if n == 0 {
				return nil
			}
		}
		ok = true

		if _, err := tr.Exec(
			`UPDATE `+GlobalName+`.workers
			 SET inflight = inflight - 1
		     WHERE id = ? AND queue = ?`, env, j.Queue); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return ok, err
	}
	return ok, nil
}

func RefreshJob(env int, j Job) (bool, error) {
	tr := db.DB()
	j.InFlight.Int64 = time.Now().Unix() + InflightLimit
	if result, err := tr.NamedExec(`UPDATE `+DatabaseName(env)+`.jobs
			SET inflight = :inflight
			WHERE id = :id AND inflight_tok = :inflight_tok`, j); err != nil {
		return false, err
	} else {
		n, _ := result.RowsAffected()
		return n == 1, nil
	}
}
