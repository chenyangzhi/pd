package index

import (
	"common"
	"encoding/binary"
	"github.com/edsrzf/mmap-go"
	"os"
)

const (
	BTNPGHEADERLENGHT = common.INT16_LEN + common.INT64_LEN +
		common.INT64_LEN + common.INT32_LEN
	PAGESIZE          = 4096
	MMAPSIZE          = 1000
	METAPAGEMAXLENGTH = 64 * 4096
	MAXPAGENUMBER     = 400000000
	DEGREE            = 110
)

type BtreeNodePage struct {
	ItemsLength    int16
	ChildrenLength int16
	PageHeader     *BtreeNodePageHeaderData
	Items          *[]*BtreeNodeItem
	ChildrenId     []int32
}

func (btreeNodePage BtreeNodePage) ToBytes() []byte {
	iStart, iEnd := int32(0), int32(0)
	bs := make([]byte, 0, PAGESIZE)
	iEnd = iStart + btreeNodePage.PageHeader.Size()
	copy(bs[iStart:iEnd], *btreeNodePage.PageHeader.ToBytes())
	iEnd = iStart + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], uint32(btreeNodePage.ItemsLength))
	iStart = iEnd
	iEnd = iEnd + common.INT32_LEN
	binary.LittleEndian.PutUint32(bs[iStart:iEnd], uint32(btreeNodePage.ChildrenLength))
	for child := range btreeNodePage.ChildrenId {
		iStart = iEnd
		iEnd = iEnd + common.INT32_LEN
		binary.LittleEndian.PutUint32(bs[iStart:iEnd], uint32(child))
	}
	arr := BatchBtreeNodeItemToBytes(btreeNodePage.Items, int32(len(*btreeNodePage.Items)))
	copy(bs[iStart:iStart+int32(len(arr))], arr)
	return bs
}

func BytesToBtreeNodePage(bs []byte) *BtreeNodePage {
	btreeNode := &BtreeNodePage{}
	iStart, iEnd := 0, 0
	iEnd = iStart + common.INT16_LEN
	btreeNode.ItemsLength = int16(binary.LittleEndian.Uint16(bs[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	btreeNode.ChildrenLength = int16(binary.LittleEndian.Uint16(bs[iStart:iEnd]))
	iStart = iEnd
	iEnd = iStart + common.INT16_LEN
	btreeNode.PageHeader = BytesToBtreeNodePageHeader(bs[iStart:iEnd])
	btreeNode.Items = BytesToBtreeNodeItems(bs[iStart:iEnd], btreeNode.ItemsLength)
	btreeNode.ChildrenId = make([]int32, btreeNode.ChildrenLength, btreeNode.ChildrenLength)
	for i := int16(0); i < btreeNode.ChildrenLength; i++ {
		iStart = iEnd
		iEnd = iStart + common.INT32_LEN
		btreeNode.ChildrenId[i] = int32(binary.LittleEndian.Uint32(bs[iStart:iEnd]))
	}
	return btreeNode
}

func GetMmapId(nodeId int32) int32 {
	return nodeId / MMAPSIZE
}

func GetMMapRegion(numberMmap int32, f *os.File) *mmap.MMap {
	off := int64(numberMmap * MMAPSIZE * PAGESIZE)
	mmp, err := mmap.MapRegion(f, PAGESIZE, mmap.RDWR, 0, off)
	common.Check(err)
	return &mmp
}

func PageToNode(bnp *BtreeNodePage, cow *copyOnWriteContext) *node {
	var n node
	n.nodeId = bnp.PageHeader.BtreeNodeId
	n.cow = cow
	for _, id := range bnp.ChildrenId {
		cNode := &childrenNode{
			childNode:   nil,
			childNodeId: id,
		}
		n.children = append(n.children, cNode)
	}
	n.items = make(items, 0, len(*bnp.Items))
	for i, o := range *bnp.Items {
		n.items[i] = o
	}
	return &n
}

func BuildBTreeFromPage(baseTableColumn string) *BTree {
	tr := New(DEGREE, baseTableColumn)
	rootId := tr.cow.mtPage.RootId
	mmapId := GetMmapId(rootId)
	m := GetMMapRegion(mmapId, tr.cow.f)
	tr.cow.mmapmap[mmapId] = m
	p := BytesToBtreeNodePage((*m)[0:1])
	tr.root = PageToNode(p, tr.cow)
	return tr
}

func GetBTreeNodeById(id int32, cow *copyOnWriteContext) *node {
	mmapId := GetMmapId(id)
	mmap := GetMMapRegion(mmapId, cow.f)
	iStart := id % MMAPSIZE * PAGESIZE
	iEnd := iStart + PAGESIZE
	pageNode := BytesToBtreeNodePage((*mmap)[iStart:iEnd])
	return PageToNode(pageNode, cow)
}
