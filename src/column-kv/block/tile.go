package block

import (
	"column-kv/column"
	"common"
)

const (
	PAGESIZE        = 4096
	MINTILESIZE     = PAGESIZE * 4
	COLUMNSIZE      = MINTILESIZE / 8
	MAXTILESIZE     = COLUMNSIZE * 65536
	COLUMNNUM       = 65536
	MAGIC           = 0x0dd6cfbc
	CRCSIZE         = 2
	TileCodeNum     = 1024
	METABLOCKSIZE   = PAGESIZE * 2
	MAXBLOCKFILENUM = 1024
	MAXTILEPAGENUM  = 16
)

type TileContent struct {
	Th       *TileHeader
	TRecodes []*TileRecode
}

func RecodeToTileRecode(idxId, version uint64, keyType byte, vlen uint16, value []byte) *TileRecode {
	return NewTileRecode(idxId, version, keyType, vlen, value)
}

func NewTileContent(param []*column.Recode) *TileContent {
	colCount := len(param)
	tileSize := uint16(0)
	vlen := uint16(0)
	colId := uint16(0)
	barr := make([]*TileRecode, 0, TileCodeNum)
	min, max := 0, 0
	for _, o := range param {
		tileSize += o.ValueSchemaSz
		vlen = o.ValueSchemaSz
		colId = o.ColumnID
		barr = append(barr, RecodeToTileRecode(o.Key, o.Timestamp, 1, o.ValueLen, o.Value))
	}
	pagenum := tileSize/PAGESIZE + 1
	th := NewTileHeader(uint32(tileSize), uint32(pagenum), uint64(min), uint64(max), vlen, colId, uint16(colCount))
	tct := new(TileContent)
	tct.Th = th
	tct.TRecodes = barr
	return tct
}

func (pc TileContent) Size() uint32 {
	return uint32(pc.Th.Size() + (pc.TRecodes[0].SizeWithoutValue()+pc.Th.ValueLength)*uint16(len(pc.TRecodes)))
}
func (pc TileContent) ToBytes(bs []byte) uint32 {
	sz := pc.Size() + uint32((len(pc.TRecodes)+1))*uint32(common.INT16_LEN)
	iStart, iEnd := uint16(0), uint16(0)
	iEnd = iStart + pc.Th.Size() + common.INT16_LEN
	pc.Th.ToBytes(bs[iStart:iEnd])
	for _, o := range pc.TRecodes {
		iStart = iEnd
		iEnd = o.SizeWithoutValue() + pc.Th.ValueLength + common.INT16_LEN
		o.ToBytes(bs[iStart:iEnd])
	}
	return sz
}
