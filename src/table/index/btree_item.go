package index

import (
	"common"
	"encoding/binary"
	"log"
)

type BtreeNodeItem struct {
	Key     string
	IdxId   int64
	KeyType byte
}

func NewBtreeNodeItem(key string, idxId int64, keyType byte) {

}

func (item BtreeNodeItem) Size() int32 {
	return int32(len(item.Key) + common.BYTE_LEN +
		common.INT16_LEN + common.INT64_LEN +
		common.INT64_LEN)
}

func (item BtreeNodeItem) ToBytes(bytes *[]byte) {
	length := item.Size()
	bs := make([]byte, 8)
	iStart, iEnd := 0, 0
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint32(bs, uint32(length))
	copy((*bytes)[iStart:iEnd], bs)
	keyLen := len(item.Key)
	iStart = iEnd
	iEnd = iStart + keyLen
	copy((*bytes)[iStart:iEnd], []byte(item.Key))
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(bs, uint64(item.IdxId))
	copy((*bytes)[iStart:iEnd], bs)
	iStart = iEnd
	iEnd = iStart + common.BYTE_LEN
	(*bytes)[iStart] = item.KeyType
	crc := common.Crc16(bs)
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	binary.LittleEndian.PutUint16(bs, crc)
	copy((*bytes)[iStart:iEnd], bs)
}

func BytesToBtreeNodeItems(barr []byte, count int16) *[]*BtreeNodeItem {
	items := make([]*BtreeNodeItem, count, count)
	iStart, iEnd := int32(0), int32(0)
	sentiel := 0
	for i := int16(0); i < count; i++ {
		iEnd = iStart + common.INT16_LEN
		length := int32(binary.LittleEndian.Uint32(barr[iStart:iEnd]))
		iStart = iEnd
		iEnd = iStart + length
		items[i].Key = string(barr[iStart:iEnd])
		iStart = iEnd
		iEnd = iStart + common.INT64_LEN
		items[i].IdxId = int64(binary.LittleEndian.Uint64(barr[iStart:iEnd]))
		iStart = iEnd
		iEnd = iStart + common.BYTE_LEN
		items[i].KeyType = barr[iStart]
		crc_0 := common.Crc16(barr[sentiel:iEnd])
		iStart = iEnd
		iEnd = iStart + common.INT16_LEN
		crc_1 := binary.LittleEndian.Uint16(barr[iStart:iEnd])
		if crc_0 != crc_1 {
			log.Fatalf("the crc is failed")
		}
		sentiel = int(iEnd)
	}
	return &items
}

func BatchBtreeNodeItemToBytes(items *[]*BtreeNodeItem, size int32) []byte {
	bytes := make([]byte, size, size)
	iStart, iEnd := int32(0), int32(0)
	for _, item := range *items {
		length := item.Size()
		iStart = iEnd
		iEnd = iStart + length
		arr := bytes[iStart:iEnd]
		item.ToBytes(&arr)
		copy(bytes[iStart:iEnd], arr)
	}
	return bytes
}
