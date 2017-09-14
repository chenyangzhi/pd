package index

import (
	"common"
	"encoding/binary"
	"log"
)

type BtreeNodePageHeaderData struct {
	PageSize    int32
	FreePointer int64
	PageVersion int16
	BtreeNodeId int32
}

func (pgHeader BtreeNodePageHeaderData) Size() int32 {
	return BTNPGHEADERLENGHT
}

func (item BtreeNodePageHeaderData) ToBytes() *[]byte {
	iStart, iEnd := 0, 0
	bs := make([]byte, 0, item.Size())
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], uint32(item.PageSize))
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(bs[iStart:iEnd], uint64(item.FreePointer))
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs[iStart:iEnd], uint16(item.PageVersion))
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(bs[iStart:iEnd], uint64(item.BtreeNodeId))
	crc := common.Crc16(bs)
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs[iStart:iEnd], crc)
	return &bs
}

func BytesToBtreeNodePageHeader(barr []byte) *BtreeNodePageHeaderData {
	iStart, iEnd := 0, 0
	item := new(BtreeNodePageHeaderData)
	iEnd = iStart + common.INT32_LEN
	item.PageSize = int32(binary.LittleEndian.Uint32(barr[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	item.FreePointer = int64(binary.LittleEndian.Uint64(barr[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	item.PageVersion = int16(binary.LittleEndian.Uint16(barr[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	item.BtreeNodeId = int32(binary.LittleEndian.Uint32(barr[iStart:iEnd]))
	crc_0 := common.Crc16(barr[0:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	crc_1 := binary.LittleEndian.Uint16(barr[iStart:iEnd])
	if crc_0 != crc_1 {
		log.Fatalf("the crc is failed")
	}
	return item
}
