package block

import (
	"unsafe"
	"encoding/binary"
	"common"
)

type Block struct {
	IndexMin    uint64
	IndexMax    uint64
	ColumOffset []uint64
	BlockTile   []*TileContent
}
type BlockIndex struct {
	IndixMin uint64
	IndixMax uint64
	Offset   uint32
}

func (bi BlockIndex) Size() uint32 {
	return uint32(unsafe.Sizeof(bi))
}

func (bi BlockIndex) ToBytes(barr []byte) uint32 {
	iStart, iEnd := uint32(0), uint32(0)
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(barr[iStart:iEnd], bi.IndixMin)
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(barr[iStart:iEnd], bi.IndixMax)
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(barr[iStart:iEnd], bi.Offset)
	return iEnd
}

type MetaBlock struct {
	Magic     uint32
	MBlockLen uint32
	BlockOff []BlockIndex
}

type BlockFile struct {
	Mb MetaBlock
	Blocks []*Block
}

func (mb MetaBlock) ToBytes()[]byte {
	bs := make([]byte,METABLOCKSIZE,METABLOCKSIZE)
	iStart, iEnd := uint32(0), uint32(0)
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], mb.Magic)
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	mb.MBlockLen = len(mb.BlockOff) * mb.BlockOff[0].Size()
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], mb.MBlockLen)
	iStart = iEnd
	for _,o := range mb.BlockOff {
		lth := o.ToBytes(bs[iStart:])
		iStart = iStart + lth
	}
	return bs
}

func (b Block) ToBytes(bs []byte) {
	iStart, iEnd := uint32(0), uint32(0)
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(bs[iStart:iEnd], b.IndexMin)
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(bs[iStart:iEnd], b.IndexMax)
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], b.ColumOffset)
	for _,o := range b.BlockTile{
		iStart = iEnd
		lth := o.ToBytes(bs[iStart:])
		iStart = iStart + lth
	}
}
