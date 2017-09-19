package index

import (
	"common"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"iowrapper"
	"os"
)

type Table struct {
	basePath   string
	table      string
	dataBase   string
	columnName string
}

func NewTable(basePath, table, dataBase, columnName string) *Table {
	return &Table{
		basePath:   basePath,
		table:      table,
		dataBase:   dataBase,
		columnName: columnName,
	}
}
func (table Table) GetTablePath() string {
	return fmt.Sprintf("%v/%v/%v/%v", table.basePath, table.dataBase, table.table, table.columnName)
}

func (table Table) CreateTable() {
	path := table.GetTablePath()
	common.Check(iowrapper.CreateSparseFile(path, 4096*10000))
	f, err := os.OpenFile(path, os.O_RDWR, 0666)
	common.Check(err)
	metaPage := NewMetaPage(0, MAXPAGENUMBER)
	bs := metaPage.ToBytes()
	mapregion, err := mmap.MapRegion(f, METAPAGEMAXLENGTH, mmap.RDWR, 0, 0)
	copy(mapregion, bs)
	mapregion.Flush()
	mapregion.Unmap()
	f.Close()
}