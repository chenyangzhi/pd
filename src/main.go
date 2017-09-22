// This binary compares memory usage between btree
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"table/index"
	"time"
	logger "until/xlog4go"
)

var (
	size = flag.Int("size", 10000000, "size of the tree to build")
)

func all(t *index.BTree) (out []index.Item) {
	t.Ascend(func(a index.Item) bool {
		out = append(out, a)
		return true
	})
	return
}
func main() {
	flag.Parse()
	if err := logger.SetupLogWithConf("/home/chenyangzhi/workplace/source/pd/conf/log.json"); err != nil {
		panic(err)
	}
	defer logger.Close()
	table := index.NewTable("/home/chenyangzhi/workplace/source/pd/data", "test", "test", "primaryKey")
	table.CreateTable()
	vals := rand.Perm(*size)
	tr := index.BuildBTreeFromPage(table.GetTablePath())
	t := time.Now()
	for _, v := range vals {
		var b index.BtreeNodeItem
		b.Key = strconv.Itoa(v)
		b.IdxId = uint64(v)
		tr.ReplaceOrInsert(b)
	}
	elapsed := time.Since(t)
	fmt.Println("the time elapsed %v", elapsed)
	t = time.Now()
	for _, v := range vals {
		var b index.BtreeNodeItem
		b.Key = strconv.Itoa(v)
		b.IdxId = uint64(v)
		idx := tr.Get(b).(index.BtreeNodeItem).IdxId
		if idx != uint64(v) {
			fmt.Println("error idx = %d val = %d", idx, v)
		}
	}
	elapsed = time.Since(t)
	fmt.Println("the time elapsed %v", elapsed)
	fmt.Println("the os page size %d", os.Getpagesize())
	fmt.Println("the tree all of node id %v", tr.GetNodeIds())
	//root := tr.GetRootNode()
	//root.Print(os.Stdout, 2)
	set := tr.GetDirtyPage()
	fmt.Println("the dirty page is %v ", set)
	os.Exit(0)

}
