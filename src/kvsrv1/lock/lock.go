package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck    kvtest.IKVClerk
	name  string
	owner string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	return &Lock{ck: ck, name: lockname, owner: kvtest.RandValue(8)}
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.name)
		if err == rpc.ErrNoKey {
			if lk.ck.Put(lk.name, lk.owner, 0) == rpc.OK {
				return
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if err != rpc.OK {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if value == lk.owner {
			return
		}

		if value == "" {
			if lk.ck.Put(lk.name, lk.owner, version) == rpc.OK {
				return
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// `Get` 一定成功拿到 Reply .
	value, version, err := lk.ck.Get(lk.name)
	if err == rpc.OK && value == lk.owner {
		lk.ck.Put(lk.name, "", version)
	}
}
