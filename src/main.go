// This binary compares memory usage between btree
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"table/index"
	"time"
)

var (
	size   = flag.Int("size", 50000, "size of the tree to build")
	degree = flag.Int("degree", 75, "degree of btree")
)

func rountine(vals *[]int) {
	tr := index.New(*degree, "./dataBase")
	for _, v := range *vals {
		tr.ReplaceOrInsert(index.Int(v))
	}
	defer Wg.Done()
}

var Wg sync.WaitGroup

func main() {
	flag.Parse()
	vals := rand.Perm(*size)
	var stats runtime.MemStats
	for i := 0; i < 10; i++ {
		runtime.GC()
	}
	fmt.Println("-------- BEFORE ----------")
	runtime.ReadMemStats(&stats)
	fmt.Printf("%+v\n", stats)
	start := time.Now()
	for i := 0; i < 1; i++ {
		Wg.Add(1)
		go rountine(&vals)
	}
	Wg.Wait()
	fmt.Printf("%v inserts in %v\n", *size, time.Since(start))
	fmt.Println("-------- AFTER ----------")
	runtime.ReadMemStats(&stats)
	fmt.Printf("%+v\n", stats)
	for i := 0; i < 10; i++ {
		runtime.GC()
	}
	fmt.Println("-------- AFTER GC ----------")
	runtime.ReadMemStats(&stats)
	fmt.Printf("%+v\n", stats)
}
