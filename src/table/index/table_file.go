package index

var TablePathMap map[string]TablePath

type TablePath struct {
	basePath   string
	table      string
	dataBase   string
	columnName string
}
