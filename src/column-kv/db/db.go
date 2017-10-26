package db

import (
	"column-kv/mem"
)

type Db struct {
	DbName string
	Tables map[string]*Table
}
type Table struct {
	TableName string
	RegionNum    uint16
	Table  []*Region
}

func NewDb() *Db {
	return &Db{
	}
}
func NewTable(TableName string,rNum uint16)*Table{
	t := new(Table)
	t.TableName = TableName
	t.RegionNum = rNum
	table := make([]*Region,rNum,rNum)
	for i := 0; i < rNum ; i++ {
		region := NewRegion()
		table[i] = region
	}
	return t
}
