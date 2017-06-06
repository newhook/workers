package workers

import (
	"math"
	"math/rand"
	"time"

	"github.com/newhook/workers/sql"
)

const (
	DEFAULT_MAX_RETRY   = 25
	NanoSecondPrecision = 1000000000.0
)

func durationToSecondsWithNanoPrecision(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / NanoSecondPrecision
}

func retry(message *sql.Job) bool {
	max := DEFAULT_MAX_RETRY
	retry := message.Retry
	count := int(message.RetryCount.Int64)
	if message.RetryMax != 0 {
		max = message.RetryMax
	}
	return retry && count < max
}

func incrementRetry(message *sql.Job) int {
	if !message.RetryCount.Valid {
		message.FailedAt.Int64 = time.Now().Unix()
		message.FailedAt.Valid = true
	} else {
		message.RetriedAt.Int64 = time.Now().Unix()
		message.RetriedAt.Valid = true
	}
	message.RetryCount.Int64++
	message.RetryCount.Valid = true

	return int(message.RetryCount.Int64)
}

func secondsToDelay(count int) int {
	power := math.Pow(float64(count), 4)
	return int(power) + 15 + (rand.Intn(30) * (count + 1))
}
