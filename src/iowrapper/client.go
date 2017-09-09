package main

import (
	"net"
	"net/rpc"
	"time"
    "fmt"
)

type (
	Client struct {
		connection *rpc.Client
	}
)

func NewClient(dsn string, timeout time.Duration) (*Client, error) {
	connection, err := net.DialTimeout("tcp", dsn, timeout)
	if err != nil {
		return nil, err
	}
	return &Client{connection: rpc.NewClient(connection)}, nil
}

func (c *Client) Get(key string) (*CacheItem, error) {
	var item *CacheItem
	err := c.connection.Call("RPC.Get", key, &item)
	return item, err
}

func (c *Client) Put(item *CacheItem) (bool, error) {
	var added bool
	err := c.connection.Call("RPC.Put", item, &added)
	return added, err
}

func (c *Client) Delete(key string) (bool, error) {
	var deleted bool
	err := c.connection.Call("RPC.Delete", key, &deleted)
	return deleted, err
}

func (c *Client) Clear() (bool, error) {
	var cleared bool
	err := c.connection.Call("RPC.Clear", true, &cleared)
	return cleared, err
}

func (c *Client) Stats() (*Requests, error) {
	requests := &Requests{}
	err := c.connection.Call("RPC.Stats", true, requests)
	return requests, err
}

var (
	c   *Client
	err error

	dsn       = "localhost:9876"
	cacheItem = &CacheItem{Key: "some key", Value: "some value"}
)

func Init() {
	c, err = NewClient(dsn, time.Millisecond*500)
	if err != nil {
		fmt.Println(err)
	}
}


func main(){
    Init()
    //c.Put(cacheItem)
	item, _ := c.Get(cacheItem.Key)
	if item != nil {
		fmt.Printf("Cache key should not exist: %s\n", cacheItem.Key)
	}
}
