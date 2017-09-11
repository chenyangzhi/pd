package manager

import "common"

type IndexField struct {
	Type     common.IndexType
	name     string
	TypeSize int16
}

type Table struct {
	Name  string
	Path  string
	Field []IndexField
}

func (table Table) CreateIndex() {

}
