package common

import "log"

const (
	INT64_LEN = 8
	INT32_LEN = 4
	INT16_LEN = 2
	BYTE_LEN  = 1
	INT8_LEN  = 1
)

type IndexType struct {
}

type Set map[int32]struct{}
func (set Set)Insert(i int32){
	set[i] = struct{}{}
}
func Check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
