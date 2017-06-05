package workers

import (
	"log"
	"strconv"

	"github.com/newhook/workers/sql"
)

func pull() []string {
	workers, err := sql.FindReady()
	if err != nil {
		log.Println("FindReady", err)
		return nil
	}
	ids := make([]string, len(workers))
	for i, w := range workers {
		ids[i] = strconv.Itoa(w.ID) + ":" + w.Queue
	}
	return ids
}
