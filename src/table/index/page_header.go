package index

import (
	"common"
	"encoding/binary"
	logger "until/xlog4go"
)

const (
	PAGEVERSION = 1
)

type BtreeNodePageHeaderData struct {
	PageSize       uint32
	ItemPointer    uint32
	PageVersion    uint16
	BtreeNodeId    uint32
	ItemsLength    uint16
	ChildrenLength uint16
	ChildrenId     []uint32
}

func (pgHeader BtreeNodePageHeaderData) Size() uint32 {
	return uint32(common.INT32_LEN*3 + common.INT16_LEN*3 + len(pgHeader.ChildrenId)*common.INT32_LEN)
}

func NewBtreeNodePageHeader(f, n uint32, i, c uint16, ci []uint32) *BtreeNodePageHeaderData {
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

func BytesToBtreeNodePageHeader(barr []byte) (*BtreeNodePageHeaderData, uint32) {
	iStart, iEnd := 0, 0
	item := new(BtreeNodePageHeaderData)
	iEnd = iStart + common.INT32_LEN
	item.PageSize = binary.LittleEndian.Uint32(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	item.ItemPointer = binary.LittleEndian.Uint32(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	item.PageVersion = binary.LittleEndian.Uint16(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	item.BtreeNodeId = binary.LittleEndian.Uint32(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	item.ItemsLength = binary.LittleEndian.Uint16(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	item.ChildrenLength = binary.LittleEndian.Uint16(barr[iStart:iEnd])
	item.ChildrenId = make([]uint32, item.ChildrenLength, item.ChildrenLength)
	for i := uint16(0); i < item.ChildrenLength; i++ {
		iStart = iEnd
		iEnd = iStart + common.INT32_LEN
		item.ChildrenId[i] = binary.LittleEndian.Uint32(barr[iStart:iEnd])
	}
	crc_0 := common.Crc16(barr[0:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	crc_1 := binary.LittleEndian.Uint16(barr[iStart:iEnd])
	if crc_0 != crc_1 {
		logger.Error("the crc is failed")
	}
	return item, item.ItemPointer
}
