package main

import (
	"log"
	"net"
	"net/rpc"
	"runtime"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rpc.Register(NewRPC())

	l, e := net.Listen("tcp", ":9876")
	if e != nil {
		log.Fatal("listen error:", e)
	}

	rpc.Accept(l)
}
