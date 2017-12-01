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
	ValueLength uint16
}
const(
	BIT = iota   //  0
	UINT8         //  1
	UINT16        //  2
	UINT32
	UINT64
	STRING
	FLOAT32
	FLOAT64
)
func (s Schema)SchemaSize()uint32{
	size := uint(0)
	for _,o := range s {
		size += o.ValueLength
	}
	return size
}
func NewField(index uint16,name string,valueType uint8,vlen uint16)*Field{
	f := new(Field)
	f.Index = index
	f.Name = name
	f.ValueType = valueType
	f.ValueLength = vlen
	return f
}
type RegionContext struct {
	mmap        map[uint32]mmap.MMap
	FilePath    string
}

func NewRegionContext(filePath string)*RegionContext{
	r := new(RegionContext)
	r.mmap = make(map[uint32]mmap.MMap)
	r.FilePath = filePath
	return r
}

type Region struct {
	RShm  *Schema
	RegionId uint16
	memTable *mem.Memtable
	Rcow  RegionContext
}

func NewRegion(rsche *Schema,rid uint16,filePath string)*Region{
	region := new(Region)
	region.RShm = rsche
	region.RegionId = rid
	region.memTable = mem.NewMemtable()
	region.Rcow = NewRegionContext(filePath)
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

