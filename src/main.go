// This binary compares memory usage between btree
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"table/index"
	"time"
)

var (
	size   = flag.Int("size", 500, "size of the tree to build")
	degree = flag.Int("degree", 8, "degree of btree")
)

func rountine(vals *[]int) {
	tr := index.New(*degree)
	for _, v := range *vals {
		tr.ReplaceOrInsert(index.Int(v))
	}
	defer Wg.Done()
}

var Wg sync.WaitGroup

func perm(n int) (out []index.Item) {
	for _, v := range rand.Perm(n) {
		out = append(out, index.Int(v))
	}
	return
}

type byInts []index.Item

func (a byInts) Len() int {
	return len(a)
}

func (a byInts) Less(i, j int) bool {
	return a[i].(index.Int) < a[j].(index.Int)
}

func (a byInts) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func testAcend() {
	arr := perm(1000)
	tr := index.New(*degree)
	for _, v := range arr {
		tr.ReplaceOrInsert(v)
	}
	sort.Sort(byInts(arr))
	for i := 0; i < 1000; i++ {
		j := 0
		tr.Ascend(func(item index.Item) bool {
			if item.(index.Int) != arr[j] {
				fmt.Println("mismatch: expected: %v, got %v", arr[j], item.(index.Int))
			}
			j++
			return true
		})
	}
}

func main() {
	flag.Parse()
	testAcend()
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
