package db

import (
	"runtime/debug"
	"testing"
	"time"
	//"database/sql"
	//"database/sql/driver"
	"github.com/go-sql-driver/mysql"

	"github.com/jmoiron/sqlx"
)

var (
	db *sqlx.DB
)

const (
	MAXIDLE = 200
	MAXOPEN = 200
	// Repeatable read.
	url = "root@/?timeout=10s&readTimeout=10s&writeTimeout=10s&multiStatements=true"
)

func init() {
	conn, err := sqlx.Open("mysql", url)
	if err != nil {
		panic(err)
	}

	if err = conn.Ping(); err != nil {
		panic(err)
	}

	// set some limits!
	conn.SetMaxIdleConns(MAXIDLE)
	conn.SetMaxOpenConns(MAXOPEN)

	db = conn
}

var (
	DB            func() Transactor                                                                                                           = realNewTransactor
	transactForDB func(db *sqlx.DB, isDeadlock func(err error) bool, pause time.Duration, maxRetries int, fn func(tx Transactor) error) error = realTransactForDB
)

// Mocks out the DB and Transact functions so that tests always run within
// the scope of their own dedicated transaction.
func Test(t *testing.T, fn func()) {
	tx, err := db.Beginx()
	if err != nil {
		t.Error(err)
	}
	DB = func() Transactor {
		return newTxWrapper(tx)
	}
	transactForDB = func(db *sqlx.DB, isDeadlock func(err error) bool, pause time.Duration, maxRetries int, fn func(tx Transactor) error) error {
		return fn(tx)
	}
	defer func() {
		DB = realNewTransactor
		transactForDB = realTransactForDB
		if err := tx.Rollback(); err != nil {
			t.Error(err)
		}
	}()

	fn()
}

func realNewTransactor() Transactor {
	return newDBWrapper(db)
}

func realTransactForDB(db *sqlx.DB, isDeadlock func(err error) bool, pause time.Duration, maxRetries int, fn func(tx Transactor) error) error {
	retries := 0
	for {
		tx, err := db.Beginx()
		if err != nil {
			return err
		}
		tr := newTxWrapper(tx)

		defer func() {
			if r := recover(); r != nil {
				debug.PrintStack()
				tx.Rollback()
				panic(r)
			}
		}()

		err = fn(tr)
		if err != nil {
			tx.Rollback()
			return err
		}

		err = tx.Commit()
		if err == nil {
			return nil
		}

		if isDeadlock(err) && retries < maxRetries {
			retries++

			// Would be nice to log this so we can detect if the database
			// is constantly deadlocking.
			time.Sleep(pause)
			continue
		}
		return err
	}
}

func Transact(fn func(tx Transactor) error) error {
	return transactForDB(db, func(err error) bool {
		if se, ok := err.(*mysql.MySQLError); ok {
			// ER_LOCK_DEADLOCK
			return se.Number == 1213
		}
		return false
	}, time.Second, 10, fn)
}
