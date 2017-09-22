package index

import (
	"common"
	"encoding/binary"
	logger "until/xlog4go"
)

type BtreeNodeItem struct {
	Key     []byte
	IdxId   uint64
	KeyType byte
}

func NewBtreeNodeItem(key []byte, idxId uint64, keyType byte)*BtreeNodeItem {
        return &BtreeNodeItem{
		Key:     key,
		IdxId:   idxId,
		KeyType: keyType,
	}
}

func (item BtreeNodeItem) Size() uint16 {
	return uint16(len(item.Key) + common.BYTE_LEN +
		common.INT16_LEN + common.INT64_LEN)
}

func (item BtreeNodeItem) KeyLength() uint16{
	return uint16(len(item.Key))
}

func (item BtreeNodeItem) ToBytes(bytes []byte) int32{
	length := item.KeyLength()
	_assert(len(bytes) >= int(item.Size()),"the BtreeNodeItem to bytes's bytes is too small")
	iStart, iEnd := 0, 0
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bytes[iStart:iEnd], length)
	keyLen := len(item.Key)
	iStart = iEnd
	iEnd = iStart + keyLen
	copy(bytes[iStart:iEnd], item.Key)
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(bytes[iStart:iEnd], uint64(item.IdxId))
	iStart = iEnd
	iEnd = iStart + common.BYTE_LEN
	bytes[iStart] = item.KeyType
	crc := common.Crc16(bytes[0:iEnd])
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bytes[iStart:iEnd], crc)
	return int32(iEnd)
}

func BytesToBtreeNodeItems(barr []byte, count uint16) []*BtreeNodeItem {
	items := make([]*BtreeNodeItem, count, count)
	iStart, iEnd := uint32(0), uint32(0)
	sentiel := 0
	for i := uint16(0); i < count; i++ {
		b := &BtreeNodeItem{}
		iStart = iEnd
		iEnd = iStart + common.INT16_LEN
		length := binary.LittleEndian.Uint16(barr[iStart:iEnd])
		iStart = iEnd
		iEnd = iStart + uint32(length)
		b.Key = barr[iStart:iEnd]
		iStart = iEnd
		iEnd = iStart + common.INT64_LEN
		b.IdxId = binary.LittleEndian.Uint64(barr[iStart:iEnd])
		iStart = iEnd
		iEnd = iStart + common.BYTE_LEN
		b.KeyType = barr[iStart]
		crc_0 := common.Crc16(barr[sentiel:iEnd])
		iStart = iEnd
		iEnd = iStart + common.INT16_LEN
		crc_1 := binary.LittleEndian.Uint16(barr[iStart:iEnd])
		if crc_0 != crc_1 {
			logger.Error("the BtreeNodeItems crc is failed")
		}
		items[i] = b
		sentiel = int(iEnd)
	}
	return items
}

func BatchBtreeNodeItemToBytes(items []*BtreeNodeItem) []byte {
	bytes := make([]byte,PAGESIZE, PAGESIZE)
	iStart, length := int32(0), int32(0)
	for _, item := range items {
		iStart = iStart + length
		length = item.ToBytes(bytes[iStart:])
	}
	return bytes[0:iStart + length]
}
