package rpc

import (
	"column-kv/db"
	"errors"
	//	"table/index"
	"fmt"
	"time"
)

type (
	RPC struct {
		Db *db.Db
		//tree     *index.BTree
		Requests *Requests
	}

	Recode struct {
		Key   int64
		Value []*[]byte
	}

	Requests struct {
		Get    uint64
		Update uint64
		Insert uint64
	}
)

var (
	NotFoundError = errors.New("key not found")
)

func NewRPC() *RPC {
	return &RPC{
		Db:       db.NewDb(),
		Requests: &Requests{},
	}
}

func (r *RPC) GetData(key int64, resp *[]*[]byte) (err error) {
	start := time.Now()
	//r.mu.RLock()
	//defer fmt.Printf("this get finished %v\n", time.Since(start))
	//defer r.mu.RUnlock()

	//cacheValue, found := r.cache[key]

	*resp = r.Db.Get(key)
	r.Requests.Get++
	fmt.Printf("this get finished %v\n", time.Since(start))
	return nil
}

func (r *RPC) InsertData(re *Recode, resp *bool) (err error) {
	key := re.Key
	val := re.Value
	*resp = r.Db.Insert(key, val)
	r.Requests.Insert++
	return nil
}

func (r *RPC) Update(re *Recode, ack *bool) error {
	key := re.Key
	val := re.Value
	*ack = r.Db.Update(key, val)
	r.Requests.Update++
	return nil
}
