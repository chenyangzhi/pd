package block

import (
	"common"
)

const (
	PAGESIZE    = 4096
	MINTILESIZE = PAGESIZE * 4
	COLUMNSIZE  = MINTILESIZE / 8
	MAXTILESIZE = COLUMNSIZE * 65536
	COLUMNNUM   = 65536
	MAGIC       = 0x0dd6cfbc
	CRCSIZE     = 2
)

type TileContent struct {
	Th       *TileHeader
	TRecodes []*TileRecode
}

func (pc TileContent) Size() uint32 {
	return pc.Th.Size() + (pc.TRecodes[0].SizeWithoutValue()+pc.Th.ValueLength)*len(pc.TRecodes)
}
func (pc TileContent) ToBytes() []byte {
	sz := pc.Size() + (len(pc.TRecodes)+1)*common.INT16_LEN
	bs := make([]byte, sz, sz)
	iStart, iEnd := 0, 0
	iEnd = iStart + pc.Th.Size() + common.INT16_LEN
	pc.Th.ToBytes(bs[iStart:iEnd])
	for _, o := range pc.TRecodes {
		iStart = iEnd
		iEnd = o.SizeWithoutValue() + pc.Th.ValueLength + common.INT16_LEN
		o.ToBytes(bs[iStart:iEnd])
	}
	return bs
}
