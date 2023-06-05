package cache

import (
	"fmt"
	"github.com/patrickmn/go-cache"
	"time"
)

type MemCache struct {
	cache *cache.Cache
}

func NewMemCache(defaultExpiration time.Duration, cleanupInterval time.Duration) *MemCache {
	return &MemCache{
		cache: cache.New(defaultExpiration*time.Minute, cleanupInterval*time.Minute),
	}
}

func (m *MemCache) Get(key string) (interface{}, error) {
	// Create a cache with a default expiration time of 5 minutes, and which
	// purges expired items every 10 minutes
	c := cache.New(cache.NoExpiration*time.Minute, 10*time.Minute)

	// Set the value of the key "foo" to "bar", with the default expiration time
	c.Set("foo", "bar", cache.DefaultExpiration)

	// Set the value of the key "baz" to 42, with no expiration time
	// (the item won't be removed until it is re-set, or removed using
	// c.Delete("baz")
	c.Set("baz", 42, cache.NoExpiration)

	// Get the string associated with the key "foo" from the cache
	foo, found := c.Get("foo")
	if found {
		fmt.Println(foo)
	}
	return foo, nil
}
