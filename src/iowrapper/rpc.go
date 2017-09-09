package main

import (
	"errors"
	"sync"
)

type (
	RPC struct {
		cache    map[string]string
		requests *Requests
		mu       *sync.RWMutex
	}

	CacheItem struct {
		Key   string
		Value string
	}

	Requests struct {
		Get    uint64
		Put    uint64
		Delete uint64
		Clear  uint64
	}
)

var (
	NotFoundError = errors.New("Cache key not found")
)

func NewRPC() *RPC {
	return &RPC{
		cache:    make(map[string]string),
		requests: &Requests{},
		mu:       &sync.RWMutex{},
	}
}

func (r *RPC) Get(key string, resp *CacheItem) (err error) {
    //start := time.Now()
    //r.mu.RLock()
    //defer fmt.Printf("this get finished %v\n", time.Since(start))
	//defer r.mu.RUnlock()
    
	//cacheValue, found := r.cache[key]

	//if !found {
    //    fmt.Printf("this get finished %v\n", time.Since(start))
	//	return NotFoundError
	//}

	*resp = CacheItem{key, "hellow"}
	r.requests.Get++
	return nil
}

func (r *RPC) Put(item *CacheItem, ack *bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.cache[item.Key] = item.Value
	*ack = true

	r.requests.Put++
	return nil
}

func (r *RPC) Delete(key string, ack *bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var found bool
	_, found = r.cache[key]

	if !found {
		return NotFoundError
	}

	delete(r.cache, key)
	*ack = true

	r.requests.Delete++
	return nil
}

func (r *RPC) Clear(skip bool, ack *bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.cache = make(map[string]string)
	*ack = true

	r.requests.Clear++
	return nil
}

func (r *RPC) Stats(skip bool, requests *Requests) error {
	*requests = *r.requests
	return nil
}
