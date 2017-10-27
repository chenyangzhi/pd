package db

import (
	"testing"
)
var Sche = make(Schema,0,4)
/*schema is uid lng lat name count */
func CreateSchema(){
	index := 0
	name := "index"
	valueType := UINT64
	vlen := 8
	f := NewField(index,name,valueType,vlen)
	Sche = append(Sche,f)
	index = 1
	name = "uid"
	valueType = UINT64
	vlen = 8
	f = NewField(index,name,valueType,vlen)
	Sche = append(Sche,f)
	index = 2
	name = "lng"
	valueType = FLOAT64
	vlen = 8
	f = NewField(index,name,valueType,vlen)
	Sche = append(Sche,f)
	index = 3
	name = "lat"
	valueType = FLOAT64
	vlen = 8
	f = NewField(index,name,valueType,vlen)
	Sche = append(Sche,f)
	index = 4
	name = "count"
	valueType = UINT32
	vlen = 4
	f = NewField(index,name,valueType,vlen)
	Sche = append(Sche,f)
}
var Reg = NewRegion(*Sche,0,"./data/region_0")
func TestRegionINsert(t *testing.T){

}