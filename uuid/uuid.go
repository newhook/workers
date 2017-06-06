package uuid

import (
	"math/rand"
	"time"

	"github.com/oklog/ulid"
)

var defaultEntropy = entropy{}

type entropy struct {
}

func (e entropy) Read(p []byte) (int, error) {
	return rand.Read(p)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func New() []byte {
	// MustNew only panics if entropy.Read panics, (which math.Read doesn't), and
	// if it is a year >= 10889, which would be bad anyway.
	id := ulid.MustNew(ulid.Timestamp(time.Now()), defaultEntropy)
	return id[:]
}

func String() string {
	// MustNew only panics if entropy.Read panics, (which math.Read doesn't), and
	// if it is a year >= 10889, which would be bad anyway.
	id := ulid.MustNew(ulid.Timestamp(time.Now()), defaultEntropy)
	return id.String()
}
