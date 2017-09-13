package index

import (
	"common"
	"github.com/edsrzf/mmap-go"
	"os"
	"encoding/binary"
)

const (
	BTNPGHEADERLENGHT = common.INT16_LEN + common.INT64_LEN +
		common.INT64_LEN + common.INT32_LEN
	PAGESIZE = 4096
	MMAPSIZE = 1000
	METAPAGEMAXLENGTH = 16*4096
	DEGREE = 110
)

var MMapmap= make(map[int64]mmap.MMap)
var MtPage = NewMetaPage(0,0)
var DirtyPage = make(common.Set)
var TableFileIo map[string]os.File

type BtreeNodePage struct {
	PageHeader  *BtreeNodePageHeaderData
	Items       *[]BtreeNodeItem
	ChildrenId    []int32
	ItemsLength int32
	ChildrenLength int32
}

func (btreeNodePage BtreeNodePage) ToBytes()[]byte{
	iStart,iEnd := 0, 0
	bs := make([]byte,0,PAGESIZE)
	iEnd = iStart + btreeNodePage.PageHeader.Size();
	btreeNodePage.PageHeader.ToBytes(bs[iStart:iEnd])
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], uint32(btreeNodePage.ItemsLength))
	iStart = iEnd; iEnd = iEnd + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], uint32(btreeNodePage.ChildrenLength))
	for child := range btreeNodePage.ChildrenId {
		iStart = iEnd; iEnd = iEnd + common.INT32_LEN
		binary.LittleEndian.PutUint32(bs[iStart:iEnd], uint32(child))
	}
	arr := BatchBtreeNodeItemToBytes(btreeNodePage.Items,len(btreeNodePage.Items))
	copy(bs[iStart:iStart + len(arr)],arr)
	return bs
}

func BytesToBtreeNodePage(bs *[]byte)*BtreeNodePage{
	return &BtreeNodePage{}
}

func GetMmapId(nodeId int32)int32{
	return nodeId/MMAPSIZE
}

func GetMMapRegion(numberMmap int32, f os.File)*mmap.MMap{
	off := numberMmap * MMAPSIZE * PAGESIZE
	mmp, err := mmap.MapRegion(f, PAGESIZE, mmap.RDWR, 0, off)
	common.Check(err)
	return &mmp
}

func PageToNode(bnp BtreeNodePage,cow *copyOnWriteContext)*node{
     	var n node
	n.nodeId = bnp.PageHeader.BtreeNodeId
     	n.cow  = cow
	for id := range bnp.ChildrenId{
		cNode := &childrenNode{
			childNode:nil,
			childNodeId:id,
		}
		n.children = cNode
	}
	n.items = bnp.Items
	return n
}

func BuildBTreeFromPage(baseTableColumn string)*BTree{
	tr := New(DEGREE)
	metaPage := GetMetaPage(baseTableColumn)
	EmptyPage[baseTableColumn] = metaPage.GetEmptyList()
	rootId := metaPage.RootId
	baseTableFileIo := TableFileIo[baseTableColumn]
	mmapId := GetMmapId(rootId)
	m :=GetMMapRegion(mmapId,baseTableFileIo)
	MMapmap[mmapId] = m
	tr.root = PageToNode(m,tr.cow)
	return tr
}

func(tr BTree) GetBTreeNodeById(id int32)*node{
        mmapId := GetMmapId(id)
        mmap := GetMMapRegion(mmapId,tr.f)
	iStart := id%MMAPSIZE * PAGESIZE;iEnd := iStart + PAGESIZE
	pageNode := BytesToBtreeNodePage(mmap[iStart:iEnd])
	return PageToNode(pageNode,tr.cow)
}