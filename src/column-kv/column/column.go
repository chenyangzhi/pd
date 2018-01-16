package column

type Recode struct {
	Key           uint64
	ValueLen      uint16
	ValueSchemaSz uint16
	Value         []byte
	ColumnID      uint16
	Timestamp     uint64
}

func NewRecode(key uint64, valueLen uint16, value []byte, columnId uint16) *Recode {
	return &Recode{
		Key:      key,
		ValueLen: valueLen,
		Value:    value,
		ColumnID: columnId,
	}
}

type Column struct {
}
