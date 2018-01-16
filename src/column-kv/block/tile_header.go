package block

import (
	"common"
	"encoding/binary"
	"unsafe"
	"until"
)

type TileHeader struct {
	TileVersion uint16
	TileSize    uint32
	PageNum     uint32
	IndexMin    uint64
	IndexMax    uint64
	ValueLength uint16
	ColumnId    uint16
	ColumnCount uint16
}

func NewTileHeader(tileSize, pagenum uint32, min, max uint64, vlen, colId, colCount uint16) *TileHeader {
	return &TileHeader{
		TileVersion: 1,
		TileSize:    tileSize,
		PageNum:     pagenum,
		IndexMin:    min,
		IndexMax:    max,
		ValueLength: vlen,
		ColumnId:    colId,
		ColumnCount: colCount,
	}
}

func (ph TileHeader) Size() uint16 {
	return uint16(unsafe.Sizeof(ph))
}

func (ph TileHeader) ToBytes(bs []byte) uint32 {
	iStart, iEnd := uint32(0), uint32(0)
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs[iStart:iEnd], ph.TileVersion)
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], ph.TileSize)
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], ph.PageNum)
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
func BytesToTileHeader(barr []byte) *TileHeader {
	iStart, iEnd := uint32(0), uint32(0)
	item := new(TileHeader)
	iEnd = iStart + common.INT16_LEN
	item.TileVersion = binary.LittleEndian.Uint16(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	item.TileSize = binary.LittleEndian.Uint32(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	item.PageNum = binary.LittleEndian.Uint32(barr[iStart:iEnd])
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
	item.TileSize = binary.LittleEndian.Uint32(barr[iStart:iEnd])
	crc_0 := common.Crc16(barr[0:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	crc_1 := binary.LittleEndian.Uint16(barr[iStart:iEnd])
	until.Assert(crc_0 == crc_1, "the TileHeader crc is failed")
	return item
}
