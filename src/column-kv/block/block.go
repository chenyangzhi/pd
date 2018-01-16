package block

import (
	"common"
	"encoding/binary"
	"unsafe"
)

type Block struct {
	IndexMin    uint64
	IndexMax    uint64
	ColumOffset []uint64
	BlockTile   []*TileContent
}

func NewBlock(min, max uint64, off []uint64, btile []*TileContent) *Block {
	b := new(Block)
	b.IndexMax = max
	b.IndexMin = min
	b.BlockTile = btile
	b.ColumOffset = off
	return b
}

type BlockIndex struct {
	IndixMin uint64
	IndixMax uint64
	Offset   uint32
}

func NewBlockIndex(max, min uint64, off uint32) *BlockIndex {
	bi := new(BlockIndex)
	bi.IndixMax = max
	bi.IndixMin = min
	bi.Offset = off
	return bi
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
	BlockOff  []BlockIndex
}

func NewMetaBlock(length uint32) *MetaBlock {
	mb := new(MetaBlock)
	mb.Magic = MAGIC
	mb.MBlockLen = length
	return mb
}

type BlockFile struct {
	Mb     *MetaBlock
	Blocks []*Block
	Size   uint32
}

func NewBlockFile(mb *MetaBlock, blocks []*Block, mbSize, blocksSize uint32) *BlockFile {
	b := new(BlockFile)
	b.Mb = mb
	b.Blocks = blocks
	size := uint32(0)
	size += mbSize
	size += blocksSize
	b.Size = size
	return b
}

func (file BlockFile) ToBytes() []byte {
	iStart, iEnd := uint32(0), uint32(0)
	bs := make([]byte, file.Size, file.Size)
	mb := file.Mb.ToBytes()
	iEnd = uint32(len(mb))
	copy(bs[iStart:iEnd], mb)
	iStart = iEnd
	for _, v := range file.Blocks {
		iStart = iStart + v.ToBytes(bs[iStart:])
	}
	return bs
}

func (mb MetaBlock) ToBytes() []byte {
	bs := make([]byte, METABLOCKSIZE, METABLOCKSIZE)
	iStart, iEnd := uint32(0), uint32(0)
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], mb.Magic)
	iStart = iEnd
	iEnd = iStart + common.INT32_LEN
	mb.MBlockLen = uint32(len(mb.BlockOff)) * mb.BlockOff[0].Size()
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], mb.MBlockLen)
	iStart = iEnd
	for _, o := range mb.BlockOff {
		lth := o.ToBytes(bs[iStart:])
		iStart = iStart + lth
	}
	return bs
}

func (b Block) ToBytes(bs []byte) uint32 {
	iStart, iEnd := uint32(0), uint32(0)
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(bs[iStart:iEnd], b.IndexMin)
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(bs[iStart:iEnd], b.IndexMax)
	iStart = iEnd
	iEnd = iStart + common.INT64_LEN
	binary.LittleEndian.PutUint64(bs[iStart:iEnd], b.ColumOffset[0])
	for _, o := range b.BlockTile {
		iStart = iEnd
		lth := o.ToBytes(bs[iStart:])
		iStart = iStart + lth
	}
	return iStart
}
