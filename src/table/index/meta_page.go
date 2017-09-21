package index

import (
	"common"
	"encoding/binary"
	"github.com/edsrzf/mmap-go"
	logger "until/xlog4go"
	"os"
)

type MetaPage struct {
	RootId         int32
	EmptyPageCount int32
	EmptyPage      []byte
}

func NewMetaPage(rootId int32, emptyCount int32) *MetaPage {
	return &MetaPage{
		RootId:         rootId,
		EmptyPageCount: emptyCount,
		EmptyPage:      make([]byte, 0, emptyCount),
	}
}

func (metaPage MetaPage) Size() int32 {
	return PAGESIZE * 64
}

func (metaPage MetaPage) ToBytes() *[]byte {
	length := metaPage.Size()
	bs := make([]byte, length)
	iStart, iEnd := 0, 0
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], uint32(metaPage.RootId))
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], uint32(metaPage.EmptyPageCount))
	iStart = iEnd
	iEnd = iStart + len(metaPage.EmptyPage)
	copy(bs[iStart:iEnd], metaPage.EmptyPage)
	crc := common.Crc16(bs)
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs[iStart:iEnd], crc)
	return &bs
}

func BytesToMetaPage(barr *[]byte) *MetaPage {
	iStart, iEnd := 0, 0
	item := new(MetaPage)
	iEnd = iStart + common.INT32_LEN
	item.RootId = int32(binary.LittleEndian.Uint32((*barr)[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	item.EmptyPageCount = int32(binary.LittleEndian.Uint32((*barr)[iStart:iEnd]))
	item.EmptyPage = make([]byte, 0, MAXPAGENUMBER)
	copy(item.EmptyPage, (*barr)[iStart:iEnd])
	crc_0 := common.Crc16((*barr)[0:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	crc_1 := binary.LittleEndian.Uint16((*barr)[iStart:iEnd])
	if crc_0 != crc_1 {
		logger.Error("the crc is failed")
	}
	return item
}

func (m MetaPage) GetEmptyList() *[]int32 {
	e := make([]int32, 0, 32)
	for i, b := range m.EmptyPage {
		if b == 0 {
			e = append(e, int32(i))
		}
	}
	return &e
}

func GetMetaPage(f *os.File) *MetaPage {
	mmp, err := mmap.MapRegion(f, METAPAGEMAXLENGTH, mmap.RDWR, 0, METAPAGEMAXLENGTH)
	bs := make([]byte, 0, len(mmp))
	copy(bs, mmp)
	common.Check(err)
	return BytesToMetaPage(&bs)
}
