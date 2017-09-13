package index

import (
	"path"
	"os"
	"common"
)

var TablePathMap map[string]TablePath

type TablePath struct {
	basePath string
	table string
	dataBase string
	columnName string
}

func OpenFileDs(basePath, table, database, columnName string){
	for key,value := range TablePathMap{
		filePath := path.Join(value.basePath, value.dataBase, value.table, value.columnName)
		f, err := os.OpenFile(filePath, os.O_RDWR, 0666)
		common.Check(err)
		key = key + "." + columnName
		TableFileIo[key] = f
	}
}