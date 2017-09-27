package block

import "unsafe"

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
	return 0
}

type MetaBlock struct {
	Magic    uint32
	BlockOff []BlockIndex
	Blocks   []*Block
}

func (mb MetaBlock) ToBytes(bs []byte) {

}

func (b Block) ToBytes(bs []byte) {

}
