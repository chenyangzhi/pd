package index

import (
	"encoding/binary"
	"common"
	"log"
)

type BtreeNodePageHeaderData struct {
	PageSize    int32
	FreePointer int64
	PageVersion int16
	BtreeNodeId int64
}

func (pgHeader BtreeNodePageHeaderData) Size() int32 {
	return BTNPGHEADERLENGHT
}

func (item BtreeNodePageHeaderData) ToBytes(bytes *[]byte) {
	iStart, iEnd := 0, 0
	bs := make([]byte, 8)
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs, uint32(item.PageSize))
	copy((*bytes)[iStart:iEnd], bs)
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(bs, uint64(item.FreePointer))
	copy((*bytes)[iStart:iEnd], bs)
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs, uint16(item.PageVersion))
	copy((*bytes)[iStart:iEnd], bs)
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(bs, uint64(item.BtreeNodeId))
	copy((*bytes)[iStart:iEnd], bs)
	crc := common.Crc16(bs)
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs, crc)
	copy((*bytes)[iStart:iEnd], bs)
}

func BytesToBtreeNodePageHeader(barr *[]byte) *BtreeNodePageHeaderData {
	iStart, iEnd := 0, 0
	item := new(BtreeNodePageHeaderData)
	iEnd = iStart + common.INT32_LEN
	item.PageSize = int32(binary.LittleEndian.Uint32((*barr)[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	item.FreePointer = int64(binary.LittleEndian.Uint64((*barr)[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	item.PageVersion = int16(binary.LittleEndian.Uint16((*barr)[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	item.BtreeNodeId = int64(binary.LittleEndian.Uint64((*barr)[iStart:iEnd]))
	crc_0 := common.Crc16((*barr)[0:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	crc_1 := binary.LittleEndian.Uint16((*barr)[iStart:iEnd])
	if crc_0 != crc_1 {
		log.Fatalf("the crc is failed")
	}
	return item
}