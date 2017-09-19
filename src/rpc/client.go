package rpc

import (
	"fmt"
	"net"
	"net/rpc"
	"time"
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

func (c *Client) GetData(key int64) (*[]*[]byte, error) {
	var item *[]*[]byte
	err := c.connection.Call("RPC.GetData", key, &item)
	return item, err
}
func (c *Client) InsertData(re *Recode) (bool, error) {
	var ack bool
	err := c.connection.Call("RPC.InsertData", re, &ack)
	return ack, err
}

//func (c *Client) Put(item *CacheItem) (bool, error) {
//	var added bool
//	err := c.connection.Call("RPC.Put", item, &added)
//	return added, err
//}

var (
	c   *Client
	err error
	dsn = "localhost:9876"
)

func Init() *Client {
	c, err = NewClient(dsn, time.Millisecond*500)
	if err != nil {
		fmt.Println(err)
	}
	return c
}
