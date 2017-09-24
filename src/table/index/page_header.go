package index

import (
	"common"
	"encoding/binary"
	"unsafe"
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
	return uint32(unsafe.Sizeof(pgHeader))
}

func NewBtreeNodePageHeader(f, n uint32, i, c uint16, ci []uint32) *BtreeNodePageHeaderData {
	return &BtreeNodePageHeaderData{
		PageSize:       BLOCKSIZE,
		ItemPointer:    f,
		PageVersion:    PAGEVERSION,
		BtreeNodeId:    n,
		ItemsLength:    i,
		ChildrenLength: c,
		ChildrenId:     ci,
	}
}

func (item BtreeNodePageHeaderData) ToBytes() []byte {
	iStart, iEnd := 0, 0
	totalSz := item.Size() + CRCSIZE
	bs := make([]byte, totalSz, totalSz)
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], item.PageSize)
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], item.ItemPointer)
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs[iStart:iEnd], item.PageVersion)
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], item.BtreeNodeId)
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs[iStart:iEnd], item.ItemsLength)
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs[iStart:iEnd], item.ChildrenLength)
	for _, v := range item.ChildrenId {
		iStart = iEnd
		iEnd = iStart + common.INT32_LEN
		binary.LittleEndian.PutUint32(bs[iStart:iEnd], v)
	}
	_assert(item.ChildrenLength == uint16(len(item.ChildrenId)), "the children length is not equal the len of the child array")
	crc := common.Crc16(bs[0:iEnd])
	iStart = iEnd
	iEnd = iStart + CRCSIZE
	binary.LittleEndian.PutUint16(bs[iStart:iEnd], crc)
	return bs
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
	_assert(crc_0 == crc_1, "the BtreeNodePageHeader crc is failed")
	return item, uint32(iEnd)
}
