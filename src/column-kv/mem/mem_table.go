package mem

import (
	"column-kv/block"
	"column-kv/column"
	"container/list"
)

type InsertMemTable [][]*column.Recode
type UpdateMemTable SkipList
type Memtable struct {
	MumEntries     int32
	MutableTable   InsertMemTable
	UpdateTable    SkipList
	MnmutableTbale *list.List
	Cur            int32
}

func NewMemtable() *Memtable {
	return &Memtable{
		MutableTable: make([][]*column.Recode, 0, 32),
	}
}

func (mem *Memtable) Add(key int64, value []*[]byte) bool {
	rcs := make([]*column.Recode, 0, len(value))
	for columnId, val := range value {
		rc := column.NewRecode(key, int16(len(*val)), val, columnId)
		rcs = append(rcs, rc)
	}
	mem.Cur++
	mem.MutableTable = append(mem.MutableTable, rcs)
	return true
}

func (mem *Memtable) Update(key int64, value []*[]byte) bool {
	rcs := make([]*column.Recode, 0, len(value))
	for columnId, val := range value {
		rc := column.NewRecode(key, int16(len(*val)), val, columnId)
		rcs = append(rcs, rc)
	}
	mem.UpdateTable.Set(key, value)
	return true
}

func (mem *Memtable) Get(key int64) (val []*[]byte) {
	val, flag := mem.GetUpdateValue(key)
	if flag != false {
		return val
	}
	return mem.GetInsertValue(key)
}

func (mem *Memtable) GetUpdateValue(key int64) (val []*[]byte, flag bool) {
	//val, flag = mem.updateTable.Get(key)
	return val, flag
}

func (mem *Memtable) GetInsertValue(key int64) (val []*[]byte) {
	i := int(mem.Cur / 2)
	end := int(mem.Cur)
	start := -1
	for i > start && i < end {
		if mem.MutableTable[i][0].Key == key {
			barr := make([]*[]byte, 0, len(mem.MutableTable[i]))
			for _, val := range mem.MutableTable[i] {
				barr = append(barr, val.Value)
			}
			return barr
		} else if mem.MutableTable[i][0].Key < key {
			start = i
			i = int((i + end + 1) / 2)
		} else {
			end = i
			i = int((start + i + 1) / 2)
		}
	}
	return nil
}

func (memtable Memtable) InsertMemTableToBlockFile() *block.BlockFile {
	bf := new(block.BlockFile)
	tile := new(block.TileContent)
	oneColumn := make([]*column.Recode, 0, block.TileCodeNum)
	count := 0
	columnIndex := 0
	columnNum := 1
	for columnIndex < columnNum {
		for _, o := range mm {
			if count == block.TileCodeNum {
				// to do  contruct the tile
				count = 0
			} else {
				oneColumn = append(oneColumn, o[columnIndex])
				count++
			}
		}
	}
	return nil
}

// to flush
func (mem Memtable) UnMutableFlush() {
	mem.MnmutableTbale
}
