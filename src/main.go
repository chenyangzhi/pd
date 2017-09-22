// This binary compares memory usage between btree
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"table/index"
	"time"
	logger "until/xlog4go"
	"encoding/binary"
)

var (
	size = flag.Int("size", 1000000, "size of the tree to build")
	logconf     = flag.String("l", "./conf/log.json", "log config file path")
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
	if err := logger.SetupLogWithConf(*logconf); err != nil {
		panic(err)
	}
	defer logger.Close()
	table := index.NewTable("/home/coco/workplace/source/pd/data", "test", "test", "primaryKey")
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
	//	tr.ReplaceOrInsert(&b)
	//}
	//elapsed = time.Since(t)
	//fmt.Println("the time elapsed ", elapsed)
	//t = time.Now()
	count := 0
	for _, v := range vals {
		var b index.BtreeNodeItem
		b.IdxId = uint64(v)
		bs := make([]byte,8,8)
		binary.LittleEndian.PutUint64(bs, uint64(b.IdxId))
		b.Key = bs
		item := tr.Get(&b)
		if item == nil {
			count++
			fmt.Println("error: not found val = ", v)
		}
	}
	elapsed := time.Since(t)
	fmt.Println("the not fount count is ",count)
	fmt.Println("the time elapsed ", elapsed)
	fmt.Println("the tree all of node id ", tr.GetNodeIds())
	//root := tr.GetRootNode()
	//root.Print(os.Stdout, 2)
	set := tr.GetDirtyPage()
	fmt.Println("the dirty page is %v ", set)
	tr.Commit()
	fmt.Println("the dirty page is commit")
	os.Exit(0)

}
