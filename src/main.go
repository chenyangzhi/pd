// This binary compares memory usage between btree
package main

import (
	"flag"
	"math/rand"
	"table/index"
)

var (
	size = flag.Int("size", 50000, "size of the tree to build")
)


func main() {
	flag.Parse()
	table := index.NewTable("/home/coco/workplace/source/pd/data","test","test","primaryKey")
	table.CreateTable()
	vals := rand.Perm(*size)
	tr := index.BuildBTreeFromPage(table.GetTablePath())
	for _, v := range vals {
		tr.ReplaceOrInsert(index.Int(v))
	}
}
