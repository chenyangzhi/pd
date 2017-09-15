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
)

func rountine(vals *[]int) {
	tr := index.BuildBTreeFromPage("./dataBase")
	for _, v := range *vals {
		tr.ReplaceOrInsert(index.Int(v))
	}
	defer Wg.Done()
}

var Wg sync.WaitGroup

func main() {
	flag.Parse()
	vals := rand.Perm(*size)
	rountine(vals)
}
