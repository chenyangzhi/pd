package block

import (
	"common"
	"encoding/binary"
	"unsafe"
	"until"
)

type PageHeader struct {
	PageVersion uint16
	PageSize    uint32
	IndexMin    uint64
	IndexMax    uint64
	ValueLength uint16
	ColumnId    uint16
	ColumnCount uint16
}

func (ph PageHeader) Size() uint16 {
	return uint16(unsafe.Sizeof(ph))
}

func (ph PageHeader) ToBytes(bs []byte) uint32 {
	iStart, iEnd := int32(0), int32(0)
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs[iStart:iEnd], ph.PageVersion)
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], ph.PageSize)
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(bs[iStart:iEnd], ph.IndexMin)
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(bs[iStart:iEnd], ph.IndexMax)
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs[iStart:iEnd], ph.ValueLength)
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs[iStart:iEnd], ph.ColumnId)
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs[iStart:iEnd], ph.ColumnCount)
	crc := common.Crc16(bs[0:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs[iStart:iEnd], crc)
	return iEnd
}
func BytesToPageHeader(barr []byte) *PageHeader {
	iStart, iEnd := 0, 0
	item := new(PageHeader)
	iEnd = iStart + common.INT16_LEN
	item.PageVersion = binary.LittleEndian.Uint16(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	item.PageSize = binary.LittleEndian.Uint32(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	item.IndexMin = binary.LittleEndian.Uint64(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	item.IndexMax = binary.LittleEndian.Uint64(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	item.ValueLength = binary.LittleEndian.Uint16(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	item.ColumnId = binary.LittleEndian.Uint16(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	item.ColumnCount = binary.LittleEndian.Uint16(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	item.PageSize = binary.LittleEndian.Uint16(barr[iStart:iEnd])
	crc_0 := common.Crc16(barr[0:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	crc_1 := binary.LittleEndian.Uint16(barr[iStart:iEnd])
	until.Assert(crc_0 == crc_1, "the PageHeader crc is failed")
	return item
}
