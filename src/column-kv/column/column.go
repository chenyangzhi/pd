package column

type Recode struct {
	Key       int64
	ValueLen  int16
	Value     *[]byte
	Timestamp int64
}

func NewRecode(key int64, valueLen int16, value *[]byte) *Recode {
	return &Recode{
		Key:      key,
		ValueLen: valueLen,
		Value:    value,
	}
}

type Column struct {
}
