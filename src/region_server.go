package main

import (
	"log"
	"net"
	nrpc "net/rpc"
	"rpc"
	"runtime"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	nrpc.Register(rpc.NewRPC())
	l, e := net.Listen("tcp", ":9876")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	nrpc.Accept(l)
}
