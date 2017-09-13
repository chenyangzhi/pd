package index

import (
	"common"
	"encoding/binary"
	"log"
	"github.com/edsrzf/mmap-go"
)
var EmptyPage = make(map[string][]int32)
type MetaPage struct {
	RootId int64
	EmptyPageCount int64
	EmptyPage []byte
	BTreePageCount int64
}

func NewMetaPage(rootId int32,emptyCount int32)*MetaPage{
	return &MetaPage{
		RootId:rootId,
		EmptyPageCount:emptyCount,
		EmptyPage:make([]int32,0,emptyCount),
	}
}


func (metaPage MetaPage) ToBytes(){
}

func BytesToMetaPage(barr *[]byte)*MetaPage  {
	iStart, iEnd := 0, 0
	item := new(MetaPage)
	iEnd = iStart + common.INT64_LEN
	item.RootId = int64(binary.LittleEndian.Uint64((*barr)[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	item.EmptyPageCount = int64(binary.LittleEndian.Uint64((*barr)[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	item.BTreePageCount = int64(binary.LittleEndian.Uint64((*barr)[iStart:iEnd]))
	item.EmptyPage = make([]byte,0, item.EmptyPageCount + item.BTreePageCount)
	copy(item.EmptyPage,(*barr)[iStart:iEnd])
	crc_0 := common.Crc16((*barr)[0:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	crc_1 := binary.LittleEndian.Uint16((*barr)[iStart:iEnd])
	if crc_0 != crc_1 {
		log.Fatalf("the crc is failed")
	}
	return item
}

func(m MetaPage)GetEmptyList()[]int32{
	e := make([]int32,0,m.EmptyPageCount)
	for i, b := range m.EmptyPage {
		if b == 0 {
			e = append(e, i)
		}
	}
	return e
}

func GetMetaPage(columnName string)*MetaPage {
	f := TableFileIo[columnName]
	mmp, err := mmap.MapRegion(f, 0, mmap.RDWR, 0, METAPAGEMAXLENGTH)
	common.Check(err)
	return BytesToMetaPage(mmp)
}