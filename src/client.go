package main

import (
	"common"
	"fmt"
	"rpc"
	"time"
)

func main() {
	value := make([]*[]byte, 0, 10)
	b := []byte("hello world")
	value = append(value, &b)
	c := rpc.Init()
	start := time.Now()
	for i := 0; i < 10000; i++ {
		r := rpc.Recode{
			Key:   int64(i),
			Value: value,
		}
		c.InsertData(&r)
	}
	fmt.Printf("this get finished %v\n", time.Since(start))
	ret, err := c.GetData(141)
	common.Check(err)
	for _, val := range *ret {
		fmt.Printf("the val is ", string(*val))
	}
}
