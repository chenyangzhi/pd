package block

import (
	"common"
	"encoding/binary"
	"unsafe"
	"until"
)

type TileRecode struct {
	IdxId       uint64
	KeyType     byte
	Version     uint64
	ValueLength uint16
	Value       []byte
}

func (pr TileRecode) Size() uint16 {
	return uint16(unsafe.Sizeof(pr))
}

func (pr TileRecode) SizeWithoutValue() uint16 {
	return uint16(unsafe.Offsetof(pr.Value))
}

func (pr TileRecode) ToBytes(bs []byte) uint32 {
	iStart, iEnd := uint32(0), uint32(0)
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(bs[iStart:iEnd], pr.IdxId)
	iStart = iEnd
	iEnd = iStart + common.INT8_LEN
	bs[iStart] = pr.KeyType
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(bs[iStart:iEnd], pr.Version)
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs[iStart:iEnd], pr.ValueLength)
	iStart = iEnd
	iEnd = iStart + uint32(pr.ValueLength)
	copy(bs[iStart:iEnd], pr.Value)
	crc := common.Crc16(bs[0:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs[iStart:iEnd], crc)
	return iEnd
}

func BytesToTileRecode(barr []byte) *TileRecode {
	iStart, iEnd := 0, 0
	item := new(TileRecode)
	iEnd = iStart + common.INT64_LEN
	item.IdxId = binary.LittleEndian.Uint64(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT8_LEN
	item.KeyType = barr[iStart]
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	item.Version = binary.LittleEndian.Uint64(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	item.ValueLength = binary.LittleEndian.Uint16(barr[iStart:iEnd])
	iStart = iEnd
	iEnd = iStart + item.ValueLength
	bs := make([]byte, item.ValueLength, item.ValueLength)
	copy(bs, barr[iStart:iEnd])
	crc_0 := common.Crc16(barr[0:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	crc_1 := binary.LittleEndian.Uint16(barr[iStart:iEnd])
	until.Assert(crc_0 == crc_1, "the TileRecode crc is failed")
	return item
}
