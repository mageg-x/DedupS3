package utils

import (
	"sync"

	"github.com/cespare/xxhash/v2"
)

const shardCount = 1024

var (
	_keylocks [shardCount]sync.Mutex
)

func For(key string) sync.Locker {
	sum := xxhash.Sum64([]byte(key))
	return &_keylocks[sum%shardCount]
}

func Lock(key string) {
	lock := For(key)
	lock.Lock()
}

func Unlock(key string) {
	lock := For(key)
	lock.Unlock()
}

func WithLockKey(key string, fn func() error) error {
	l := For(key)
	l.Lock()
	defer l.Unlock()
	return fn()
}
