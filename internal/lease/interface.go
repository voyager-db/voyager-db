package lease

import "time"

// Info mirrors TTL info for LeaseTimeToLive.
type Info struct {
	ID        int64
	TTL       int64
	ExpireAt  time.Time
	Remaining int64
}

// ManagerAPI is the common surface for lease managers.
type ManagerAPI interface {
	// core
	Grant(ttlSec int64) (id int64, expireAt time.Time)
	KeepAlive(id int64) (remaining int64, ttl int64, err error)
	TTL(id int64) (Info, error)
	Revoke(id int64) error
	Close()

	// key attachment
	AttachKey(id int64, key []byte) error
	DetachKey(id int64, key []byte) error
	Keys(id int64) ([][]byte, error)

	// expiry notifications (push model)
	Expired() <-chan int64
}
