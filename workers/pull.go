package workers

import (
	"log"
	"strconv"

	"github.com/newhook/workers/fair"
	"github.com/newhook/workers/sql"
)

func pull() []fair.Work {
	workers, err := sql.FindReady()
	if err != nil {
		log.Println("FindReady", err)
		return nil
	}
	ids := make([]fair.Work, len(workers))
	for i, w := range workers {
		ids[i] = fair.Work{
			ID:   strconv.Itoa(w.ID) + ":" + w.Queue,
			Data: &w,
		}
	}
	return ids
}
