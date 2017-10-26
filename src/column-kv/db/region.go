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
	FilePath    string
}
type Region struct {
	RShm  Schema
	RegionId uint16
	memTable mem.Memtable
	Rcow  RegionContext
}

func NewRegion()*Region{
	return new(Region)
}

func (region Region)Insert(key int64, value []*[]byte)bool{
	return region.memTable.Add(key,value)
}

func (region Region)Update(key int64, value []*[]byte)bool{
	return region.memTable.Update(key,value)
}

func (region Region)Get(key int64){
	val := region.memTable.Get(key)
	if val != nil {
		return val
	}else {
		//to do: find the in the disk
		return nil
	}
}

