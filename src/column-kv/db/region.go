package db

import (
	"column-kv/mem"
	"github.com/edsrzf/mmap-go"
)

type Schema []Field
type Field struct {
	Index       uint16
	Name        string
	ValueType   uint8
	ValueLenght uint16
}
type RegionContext struct {
	mmap        map[uint32]mmap.MMap
	IminsertTab map[uint32]mem.InsertMemTable
	ImUpdateTab mem.UpdateMemTable
}
type Region struct {
	RShm  Schema
	RRMin uint64
	RRMax uint64
	RItab mem.InsertMemTable
	RUtab mem.UpdateMemTable
	Rcow  RegionContext
}
