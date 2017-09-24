package block

import "unsafe"

type Block struct {
	Colnum         uint16
	IndexMin       uint64
	IndexMax       uint64
	BlockId        uint64
	ColumOffset    []uint64
	BlockPage      []*PageContent
}
type BlockIndex struct {
	IndixMin uint64
	IndixMax uint64
	Offset   uint32
}

func (bi BlockIndex) Size()uint32{
	return uint32(unsafe.Sizeof(bi))
}

func (bi BlockIndex)ToBytes(barr []byte)uint32{
	return 0
}


type MetaBlock struct {
	Magic    uint32
	BlockNum uint32
	BlockSize   uint64
	BlockOff   []BlockIndex
	Blocks     []*Block
}

func (mb MetaBlock) Size()uint32{
	return PAGESIZE
}

func (mb MetaBlock) ToBytes(bs []byte){

}

func (b Block) Size()uint32{
	return
}

func (b Block)ToBytes(bs []byte){

}