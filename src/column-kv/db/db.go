package db

import (
	"column-kv/mem"
)

type Db struct {
	DbName string
	Mem    *mem.Memtable
	Table  []*Region
}

func NewDb() *Db {
	return &Db{
		Mem: mem.NewMemtable(),
	}
}
func (db *Db) Insert(key int64, columns []*[]byte) bool {
	flag := db.Mem.Add(key, columns)
	return flag
}
func (db *Db) Update(key int64, columns []*[]byte) bool {
	return db.Mem.Update(key, columns)
}
func (db *Db) Get(key int64) []*[]byte {
	return db.Mem.Get(key)
}
