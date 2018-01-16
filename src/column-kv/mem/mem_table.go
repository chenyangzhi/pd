package mem

import (
	"column-kv/block"
	"column-kv/column"
	"container/list"
	"iowrapper"
)

const (
	MAXMEMTABSIZE = 64 * 1024 * 1024
	UNMUTABLENUM  = 4
)

type BlockFileInfo struct {
	MaxId    uint64
	MinId    uint64
	Useage   float32
	FileName string
}

type InsertMemTable [][]*column.Recode
type UpdateMemTable SkipList
type Memtable struct {
	MumEntries     int32
	MutableTabSize int32
	MutableTable   InsertMemTable
	UpdateTable    SkipList
	MnmutableTbale *Queue
	Bf             *block.BlockFile
	BlockCache     *list.List
	File           *BlockFileInfo
	Cur            int32
}

func NewMemtable() *Memtable {
	return &Memtable{
		MutableTable: make([][]*column.Recode, 0, 32),
	}
}

func (mem *Memtable) Add(key uint64, value []*[]byte) bool {
	rcs := make([]*column.Recode, 0, len(value))
	for columnId, val := range value {
		mem.MutableTabSize += int32(len(*val))
		rc := column.NewRecode(key, uint16(len(*val)), *val, uint16(columnId))
		rcs = append(rcs, rc)
	}
	mem.Cur++
	mem.MutableTable = append(mem.MutableTable, rcs)
	if mem.MutableTabSize > MAXMEMTABSIZE {
		mem.MnmutableTbale.Add(mem.MutableTable)
		if mem.MnmutableTbale.Len() > UNMUTABLENUM {
			go mem.UnMutableFlush()
		}
	}
	return true
}

func (mem *Memtable) Update(key uint64, value []*[]byte) bool {
	rcs := make([]*column.Recode, 0, len(value))
	for columnId, val := range value {
		rc := column.NewRecode(key, uint16(len(*val)), *val, uint16(columnId))
		rcs = append(rcs, rc)
	}
	mem.UpdateTable.Set(key, value)
	return true
}

func (mem *Memtable) Get(key uint64) (val []*[]byte) {
	val, flag := mem.GetUpdateValue(key)
	if flag != false {
		return val
	}
	return mem.GetInsertValue(key)
}

func (mem *Memtable) GetUpdateValue(key uint64) (val []*[]byte, flag bool) {
	//val, flag = mem.updateTable.Get(key)
	return val, flag
}

func (mem *Memtable) GetInsertValue(key uint64) (val []*[]byte) {
	i := int(mem.Cur / 2)
	end := int(mem.Cur)
	start := -1
	for i > start && i < end {
		if mem.MutableTable[i][0].Key == key {
			barr := make([]*[]byte, 0, len(mem.MutableTable[i]))
			for _, val := range mem.MutableTable[i] {
				barr = append(barr, &(val.Value))
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

func (memtable InsertMemTable) UnMutableMemTableToBlockFile(bf *block.BlockFile) *block.BlockFile {
	if bf == nil {
		bf = new(block.BlockFile)
	}
	oneColumn := make([]*column.Recode, 0, block.TileCodeNum)
	count := 0
	columnIndex := 0
	columnNum := 1
	blockth := 0
	for columnIndex < columnNum {
		for _, o := range memtable {
			if count == block.TileCodeNum {
				// to do  contruct the tile
				//tile := block.NewTileContent(oneColumn)
				//bf.Blocks[blockth].BlockTile[columnIndex].Th = tile
				count = 0
			} else {
				oneColumn = append(oneColumn, o[columnIndex])
				count++
			}
		}
		columnIndex++
	}
	if blockth > block.MAXBLOCKFILENUM {
		bs := bf.ToBytes()
		iowrapper.WriteFile("", bs)
	}
	return bf
}

// to flush
func (mem Memtable) UnMutableFlush() {
	l := mem.MnmutableTbale
	for e := l.Next(); e != nil; {
		mem.Bf = e.(InsertMemTable).UnMutableMemTableToBlockFile(mem.Bf)
	}
}
