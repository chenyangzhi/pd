// This binary compares memory usage between btree
package main

import (
	"common"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/xwb1989/sqlparser"
	"log"
	"math/rand"
	"os"
	"runtime"
	"table/index"
	"time"
	logger "until/xlog4go"
)

var (
	size    = flag.Int("size", 10000000, "size of the tree to build")
	logconf = flag.String("l", "./conf/log.json", "log config file path")
)

func all(t *index.BTree) (out []index.Item) {
	t.Ascend(func(a index.Item) bool {
		out = append(out, a)
		return true
	})
	return
}

func memery() {
	for {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		log.Printf("Alloc = %vMB  TotalAlloc = %vMB  Sys = %vMB  NumGC = %vMB\n", m.Alloc/(1024*1024), m.TotalAlloc/(1024*1024), m.Sys/(1024*1024), m.NumGC)
		time.Sleep(5 * time.Second)
	}
}

func main() {
	flag.Parse()
	if err := logger.SetupLogWithConf(*logconf); err != nil {
		panic(err)
	}
	defer logger.Close()
	sql := "select a from tabke"
	st, err := sqlparser.Parse(sql)
	common.Check(err)
	fmt.Println("the sqlparser is %v", st)
	go memery()
	table := index.NewTable("/home/chenyangzhi/workplace/source/pd/data", "test", "test", "primaryKey")
	table.CreateTable()
	vals := rand.Perm(*size)
	tr := index.BuildBTreeFromPage(table.GetTablePath())

	t := time.Now()
	//for _, v := range vals {
	//	var b index.BtreeNodeItem
	//	bs := make([]byte,8,8)
	//	b.IdxId = uint64(v)
	//	binary.LittleEndian.PutUint64(bs, uint64(b.IdxId))
	//	b.Key = bs
	//	if b.IdxId == 178 {
	//		tr.ReplaceOrInsert(&b)
	//	}else{
	//		tr.ReplaceOrInsert(&b)
	//	}
	//	item := tr.Get(&b)
	//	if item == nil {
	//		fmt.Println("error: not insert val = ", v)
	//	}
	//}
	//elapsed := time.Since(t)
	//fmt.Println("the time elapsed ", elapsed)
	//t = time.Now()
	count := 0
	for _, v := range vals {
		var b index.BtreeNodeItem
		b.IdxId = uint64(v)
		bs := make([]byte, 8, 8)
		binary.LittleEndian.PutUint64(bs, uint64(b.IdxId))
		b.Key = bs
		item := tr.Get(&b)
		if item == nil {
			count++
			fmt.Println("error: not found val = ", v)
		}
	}
	elapsed := time.Since(t)
	//root := tr.GetRootNode()
	//root.Print(os.Stdout, 2)
	fmt.Println("the not fount count is ", count)
	fmt.Println("the time elapsed ", elapsed)
	fmt.Println("the tree all of node id ", tr.GetNodeIds())
	set := tr.GetDirtyPage()
	fmt.Println("the dirty page is %v ", set)
	tr.Commit()
	fmt.Println("the dirty page is commit")
	os.Exit(0)

}
