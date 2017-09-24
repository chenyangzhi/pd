package block

import (
	"common"
)

const (
	PAGESIZE  = 4096 * 2
	MMAPSIZE  = 200
	MAGIC     = 0x0dd6cfbc
	CRCSIZE   = 2
	BLOCKSIZE = 4096 * 2
)

type PageContent struct {
	Ph       *PageHeader
	PRecodes []*PageRecode
}

func (pc PageContent) Size() uint32 {
	return pc.Ph.Size() + (pc.PRecodes[0].SizeWithoutValue()+pc.Ph.ValueLength)*len(pc.PRecodes)
}
func (pc PageContent) ToBytes() []byte {
	sz := pc.Size() + (len(pc.PRecodes)+1)*common.INT16_LEN
	bs := make([]byte, sz, sz)
	iStart, iEnd := 0, 0
	iEnd = iStart + pc.Ph.Size() + common.INT16_LEN
	pc.Ph.ToBytes(bs[iStart:iEnd])
	for _, o := range pc.PRecodes {
		iStart = iEnd
		iEnd = o.SizeWithoutValue() + pc.Ph.ValueLength + common.INT16_LEN
		o.ToBytes(bs[iStart:iEnd])
	}
	return bs
}
