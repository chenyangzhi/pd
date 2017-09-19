package index

import (
	"common"
	"encoding/binary"
	"log"
)

const (
	PAGEVERSION = 1
)

type BtreeNodePageHeaderData struct {
	PageSize       int32
	ItemPointer    int32
	PageVersion    int16
	BtreeNodeId    int32
	ItemsLength    int16
	ChildrenLength int16
	ChildrenId     []int32
}

func (pgHeader BtreeNodePageHeaderData) Size() int32 {
	return common.INT32_LEN*3 + common.INT16_LEN*3 + len(pgHeader.ChildrenId)*common.INT32_LEN
}

func NewBtreeNodePageHeader(f, n int32, i, c int16, ci []int32) *BtreeNodePageHeaderData {
	return &BtreeNodePageHeaderData{
		PageSize:       PAGESIZE,
		ItemPointer:    f,
		PageVersion:    PAGEVERSION,
		BtreeNodeId:    n,
		ItemsLength:    i,
		ChildrenLength: c,
		ChildrenId:     ci,
	}
}

func (item BtreeNodePageHeaderData) ToBytes() *[]byte {
	iStart, iEnd := 0, 0
	bs := make([]byte, 0, item.Size())
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], uint32(item.PageSize))
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], uint32(item.ItemPointer))
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs[iStart:iEnd], uint16(item.PageVersion))
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], uint32(item.BtreeNodeId))
	crc := common.Crc16(bs)
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs[iStart:iEnd], crc)
	return &bs
}

func BytesToBtreeNodePageHeader(barr []byte) (*BtreeNodePageHeaderData, int32) {
	iStart, iEnd := 0, 0
	item := new(BtreeNodePageHeaderData)
	iEnd = iStart + common.INT32_LEN
	item.PageSize = int32(binary.LittleEndian.Uint32(barr[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	item.ItemPointer = int32(binary.LittleEndian.Uint32(barr[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	item.PageVersion = int16(binary.LittleEndian.Uint16(barr[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	item.BtreeNodeId = int32(binary.LittleEndian.Uint32(barr[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	item.ItemsLength = int16(binary.LittleEndian.Uint16(barr[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	item.ChildrenLength = int16(binary.LittleEndian.Uint16(barr[iStart:iEnd]))
	item.ChildrenId = make([]int32, item.ChildrenLength, item.ChildrenLength)
	for i := int16(0); i < item.ChildrenLength; i++ {
		iStart = iEnd
		iEnd = iStart + common.INT32_LEN
		item.ChildrenId[i] = int32(binary.LittleEndian.Uint32(barr[iStart:iEnd]))
	}
	crc_0 := common.Crc16(barr[0:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	crc_1 := binary.LittleEndian.Uint16(barr[iStart:iEnd])
	if crc_0 != crc_1 {
		log.Fatalf("the crc is failed")
	}
	return item, item.ItemPointer
}
