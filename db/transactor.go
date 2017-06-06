package db

import (
	"database/sql"
	"fmt"
	"net/http"
	"runtime/debug"

	"github.com/jmoiron/sqlx"
)

type Transactor interface {
	// Rebind transforms a query from QUESTION to the DB driver's bindvar type.
	Rebind(query string) string

	// Exec executes a query that doesn't return rows.
	// For example: an INSERT and UPDATE.
	Exec(query string, args ...interface{}) (sql.Result, error)

	// Prepare creates a prepared statement for later queries or executions.
	// Multiple queries or executions may be run concurrently from the
	// returned statement.
	// The caller must call the statement's Close method
	// when the statement is no longer needed.
	Prepare(query string) (*sql.Stmt, error)

	// Query executes a query that returns rows, typically a SELECT.
	Query(query string, args ...interface{}) (*sql.Rows, error)

	// QueryRow executes a query that is expected to return at most one row.
	// QueryRow always returns a non-nil value. Errors are deferred until
	// Row's Scan method is called.
	QueryRow(query string, args ...interface{}) *sql.Row

	// BindNamed binds a query using the DB driver's bindvar type.
	BindNamed(query string, arg interface{}) (string, []interface{}, error)

	// NamedQuery using this DB.
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)

	// NamedExec using this DB.
	NamedExec(query string, arg interface{}) (sql.Result, error)

	// Select using this DB.
	Select(dest interface{}, query string, args ...interface{}) error

	// Get using this DB.
	Get(dest interface{}, query string, args ...interface{}) error

	// Queryx queries the database and returns an *sqlx.Rows.
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)

	// QueryRowx queries the database and returns an *sqlx.Row.
	QueryRowx(query string, args ...interface{}) *sqlx.Row

	// MustExec (panic) runs MustExec using this database.
	MustExec(query string, args ...interface{}) sql.Result

	// Preparex returns an sqlx.Stmt instead of a sql.Stmt
	Preparex(query string) (*sqlx.Stmt, error)

	// PrepareNamed returns an sqlx.NamedStmt
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
}

var (
	TraceSQL = false
)

func init() {
	http.HandleFunc("/debug/trace_sql", func(w http.ResponseWriter, req *http.Request) {
		TraceSQL = !TraceSQL
		fmt.Fprintf(w, "tracing is now %v", TraceSQL)
	})
}

func simplify(args []interface{}) []interface{} {
	r := make([]interface{}, len(args))
	for i, a := range args {
		switch v := a.(type) {
		case []byte:
			if len(v) > 32 {
				r[i] = fmt.Sprintf("(%d bytes) %s...%s", len(v), string(v[0:10]), string(v[len(v)-10:]))
			} else {
				r[i] = string(v)
			}
		default:
			r[i] = v
		}
	}
	return r
}

type dbWrapper struct {
	//ctx ciocontext.Context
	db *sqlx.DB

	//stats *RequestStats
	//tr    trace.Trace
}

func newDBWrapper(db *sqlx.DB) *dbWrapper {
	d := &dbWrapper{
		//ctx: ctx,
		db: db,
	}
	/*
		if tr, ok := trace.FromContext(ctx); ok && TraceSQL {
			d.tr = tr
		}
		if v, ok := ctx.GetValue(statsContextKey); ok {
			d.stats = v.(*RequestStats)
		}
	*/
	return d
}

func (db *dbWrapper) trace(err error, s string, args ...interface{}) {
	/*
		if err != nil {
			fmt.Printf(s, args...)
			fmt.Println()
			if err != nil {
				fmt.Println(err)
				debug.PrintStack()
			}
		}
	*/
	if TraceSQL {
		fmt.Printf(s, args...)
		fmt.Println()
		if err != nil {
			fmt.Println(err)
			debug.PrintStack()
		}
	}
	/*
		if db.tr != nil {
			db.tr.LazyPrintf(s, args...)
			if err != nil {
				db.tr.LazyPrintf("error: %v %s", err, string(debug.Stack()))
			}
		}
	*/
}

func (db *dbWrapper) query() {
	/*
		if db.stats != nil {
			db.stats.Query++
		}
	*/
}

func (db *dbWrapper) exec() {
	/*
		if db.stats != nil {
			db.stats.Exec++
		}
	*/
}

func (db *dbWrapper) Rebind(query string) string {
	return db.db.Rebind(query)
}

func (db *dbWrapper) Exec(query string, args ...interface{}) (sql.Result, error) {
	db.exec()
	r, err := db.db.Exec(query, args...)
	db.trace(err, "sql.Exec %s %v (%v %v)", query, simplify(args), r, err)
	return r, err
}

func (db *dbWrapper) Prepare(query string) (*sql.Stmt, error) {
	s, err := db.db.Prepare(query)
	db.trace(err, "sql.Prepare %s", query)
	return s, err
}

func (db *dbWrapper) Query(query string, args ...interface{}) (*sql.Rows, error) {
	db.query()
	r, err := db.db.Query(query, args...)
	db.trace(err, "sql.Query %s %v", query, simplify(args))
	return r, err
}

func (db *dbWrapper) QueryRow(query string, args ...interface{}) *sql.Row {
	db.query()
	r := db.db.QueryRow(query, args...)
	db.trace(nil, "sql.QueryRow: %s %v", query, simplify(args))
	return r
}

func (db *dbWrapper) BindNamed(query string, arg interface{}) (string, []interface{}, error) {
	return db.db.BindNamed(query, arg)
}

func (db *dbWrapper) NamedQuery(query string, arg interface{}) (*sqlx.Rows, error) {
	db.query()

	r, err := db.db.NamedQuery(query, arg)
	db.trace(err, "sql.NamedQuery: %s %+v", query, arg)
	return r, err
}

func (db *dbWrapper) NamedExec(query string, arg interface{}) (sql.Result, error) {
	db.exec()

	r, err := db.db.NamedExec(query, arg)
	db.trace(err, "sql.NamedExec: %s %+v", query, arg)
	return r, err
}

func (db *dbWrapper) Select(dest interface{}, query string, args ...interface{}) error {
	db.query()
	err := db.db.Select(dest, query, args...)
	db.trace(err, "sql.Select: %+v %s %v", dest, query, simplify(args))
	return err
}

func (db *dbWrapper) Get(dest interface{}, query string, args ...interface{}) error {
	db.query()

	err := db.db.Get(dest, query, args...)
	db.trace(err, "sql.Get: %+v %s %v", dest, query, simplify(args))
	return err
}

func (db *dbWrapper) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	db.query()

	r, err := db.db.Queryx(query, args...)
	db.trace(err, "sql.Queryx: %s %v", query, simplify(args))
	return r, err
}

func (db *dbWrapper) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	db.query()

	r := db.db.QueryRowx(query, args...)
	db.trace(nil, "sql.QueryRowx: %s %v", query, simplify(args))
	return r
}

func (db *dbWrapper) MustExec(query string, args ...interface{}) sql.Result {
	db.exec()
	r := db.db.MustExec(query, args...)
	db.trace(nil, "sql.MustExec %s %v", query, simplify(args))
	return r
}

func (db *dbWrapper) Preparex(query string) (*sqlx.Stmt, error) {
	return db.db.Preparex(query)
}

func (db *dbWrapper) PrepareNamed(query string) (*sqlx.NamedStmt, error) {
	return db.db.PrepareNamed(query)
}

type txWrapper struct {
	tx *sqlx.Tx
}

func newTxWrapper(tx *sqlx.Tx) *txWrapper {
	return &txWrapper{
		tx: tx,
	}
}

func (t *txWrapper) trace(err error, s string, args ...interface{}) {
	/*
		if err != nil {
			fmt.Printf(s, args...)
			fmt.Println()
			if err != nil {
				fmt.Println(err)
				debug.PrintStack()
			}
		}
	*/

	if TraceSQL {
		fmt.Printf(s, args...)
		fmt.Println()
		if err != nil {
			fmt.Println(err)
			debug.PrintStack()
		}
	}
	/*
		if t.tr != nil {
			t.tr.LazyPrintf(s, args...)
			if err != nil {
				t.tr.LazyPrintf("error: %v %s", err, string(debug.Stack()))
			}
		}
	*/
}

func (t *txWrapper) query() {
	/*
		if t.stats != nil {
			t.stats.Query++
		}
		if t.txs != nil {
			t.txs.Query++
		}
	*/
}

func (t *txWrapper) exec() {
	/*
		if t.stats != nil {
			t.stats.Exec++
		}
		if t.txs != nil {
			t.txs.Exec++
		}
	*/
}

func (t *txWrapper) Exec(query string, args ...interface{}) (sql.Result, error) {
	t.exec()
	r, err := t.tx.Exec(query, args...)
	t.trace(err, "sql.Exec %s %v (%v %v)", query, simplify(args), r, err)

	return r, err
}

func (t *txWrapper) Prepare(query string) (*sql.Stmt, error) {
	s, err := t.tx.Prepare(query)
	t.trace(err, "sql.Prepare %s", query)
	return s, err
}

func (t *txWrapper) Query(query string, args ...interface{}) (*sql.Rows, error) {
	t.query()

	r, err := t.tx.Query(query, args...)
	t.trace(err, "sql.Query %s %v", query, simplify(args))
	return r, err
}

func (t *txWrapper) QueryRow(query string, args ...interface{}) *sql.Row {
	t.query()
	r := t.tx.QueryRow(query, args...)
	t.trace(nil, "sql.QueryRow: %s %v", query, simplify(args))
	return r
}

func (t *txWrapper) Stmt(stmt *sql.Stmt) *sql.Stmt {
	return t.tx.Stmt(stmt)
}

func (t *txWrapper) Rebind(query string) string {
	return t.tx.Rebind(query)
}

func (t *txWrapper) Unsafe() *sqlx.Tx {
	return nil
}

func (t *txWrapper) BindNamed(query string, arg interface{}) (string, []interface{}, error) {
	return t.tx.BindNamed(query, arg)
}

func (t *txWrapper) NamedQuery(query string, arg interface{}) (*sqlx.Rows, error) {
	t.query()
	r, err := t.tx.NamedQuery(query, arg)
	t.trace(err, "sql.NamedQuery: %s %+v", query, arg)
	return r, err
}

func (t *txWrapper) NamedExec(query string, arg interface{}) (sql.Result, error) {
	t.exec()
	r, err := t.tx.NamedExec(query, arg)
	t.trace(err, "sql.NamedExec %s %+v", query, arg)
	return r, err
}

func (t *txWrapper) Select(dest interface{}, query string, args ...interface{}) error {
	t.query()

	err := t.tx.Select(dest, query, args...)
	t.trace(err, "sql.Select: %+v %s %v", dest, query, simplify(args))
	return err
}

func (t *txWrapper) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	t.query()
	r, err := t.tx.Queryx(query, args...)
	t.trace(err, "sql.Queryx: %s %v", query, simplify(args))
	return r, err
}

func (t *txWrapper) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	t.query()

	r := t.tx.QueryRowx(query, args...)
	t.trace(nil, "sql.QueryRowx: %s %v", query, simplify(args))
	return r
}

func (t *txWrapper) Get(dest interface{}, query string, args ...interface{}) error {
	t.query()

	err := t.tx.Get(dest, query, args...)
	t.trace(err, "sql.Get: %+v %s %v", dest, query, simplify(args))
	return err
}

func (t *txWrapper) MustExec(query string, args ...interface{}) sql.Result {
	t.exec()
	r := t.tx.MustExec(query, args...)
	t.trace(nil, "sql.MustExec %s %v", query, simplify(args))
	return r
}

func (t *txWrapper) Preparex(query string) (*sqlx.Stmt, error) {
	return t.tx.Preparex(query)
}

func (t *txWrapper) Stmtx(stmt interface{}) *sqlx.Stmt {
	return t.tx.Stmtx(stmt)
}

func (t *txWrapper) NamedStmt(stmt *sqlx.NamedStmt) *sqlx.NamedStmt {
	return t.tx.NamedStmt(stmt)
}

func (t *txWrapper) PrepareNamed(query string) (*sqlx.NamedStmt, error) {
	return t.tx.PrepareNamed(query)
}
