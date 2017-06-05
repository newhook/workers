package sql

import (
	"database/sql"
	"log"
	"sort"
	"strconv"
	"strings"

	"github.com/newhook/workers/db"
)

var GlobalStructure = `
CREATE DATABASE __DBNAME__ CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE __DBNAME__.workers (
  id INT NOT NULL,
  queue varchar(30) NOT NULL,
  count bigint(20) DEFAULT 0,
  PRIMARY KEY(queue, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
`

var EnvStructure = `
CREATE DATABASE __DBNAME__ CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE __DBNAME__.jobs (
  id SERIAL,
  queue varchar(30) NOT NULL,
  data VARBINARY(60000) NOT NULL,
  in_flight INT,
  created_at INT NOT NULL,
  updated_at INT NOT NULL,
  KEY queue_index (queue)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
`

var (
	GlobalName = "workers_shard"
	LocalName  = "workers_env_"
)

func DatabaseName(id int) string {
	return LocalName + strconv.Itoa(id)
}

func SetupGlobal(tr db.Transactor) error {
	structure := strings.Replace(GlobalStructure, "__DBNAME__", GlobalName, -1)
	if _, err := tr.Exec(structure); err != nil {
		return err
	}
	return nil
}

func EnvironmentIDs(tr db.Transactor) ([]int, error) {
	rows, err := tr.Query("show databases;")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Println("rows.Close failed", err)
		}
	}()

	dbname := LocalName
	var ids []int

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if strings.HasPrefix(name, dbname) {
			id, _ := strconv.Atoi(name[len(dbname):])
			ids = append(ids, id)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Ints(ids)

	return ids, nil
}

func Reset(tr db.Transactor) error {
	ids, err := EnvironmentIDs(tr)
	if err != nil {
		return err
	}

	for _, id := range ids {
		if _, err := tr.Exec("DROP DATABASE " + DatabaseName(id)); err != nil {
			return err
		}
	}

	if _, err := tr.Exec("DROP DATABASE IF EXISTS " + GlobalName); err != nil {
		return err
	}

	return nil
}

func MaybeSetupGlobal(tr db.Transactor) error {
	var name string

	err := tr.QueryRow(`
    SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '` + GlobalName + `';
	`).Scan(&name)

	if err != nil {
		if err == sql.ErrNoRows {
			return SetupGlobal(tr)
		}

		return err
	}
	return nil
}

func SetupEnv(tr db.Transactor, id int) error {
	dbname := DatabaseName(id)

	if err := MaybeSetupGlobal(tr); err != nil {
		return err
	}

	structure := strings.Replace(EnvStructure, "__DBNAME__", dbname, -1)
	if _, err := tr.Exec(structure); err != nil {
		return err
	}

	return nil
}
