package index

import (
	"common"
	"github.com/edsrzf/mmap-go"
	"os"
)

const (
	PAGESIZE          = 4096
	MMAPSIZE          = 1000
	METAPAGEMAXLENGTH = 64 * 4096
	MAXPAGENUMBER     = 2093056
	DEGREE            = 110
)

type BtreeNodePage struct {
	PageHeader *BtreeNodePageHeaderData
	Items      *[]*BtreeNodeItem
}

func NewBreeNodePage(p *BtreeNodePageHeaderData, i *[]*BtreeNodeItem) *BtreeNodePage {
	return &BtreeNodePage{
		PageHeader: p,
		Items:      i,
	}
}

func (btreeNodePage BtreeNodePage) ToBytes() []byte {
	iStart, iEnd := int32(0), int32(0)
	bs := make([]byte, 0, PAGESIZE)
	bp := btreeNodePage.PageHeader.ToBytes()
	iEnd = iStart + int32(len(*bp))
	copy(bs[iStart:iEnd], bs)
	arr := BatchBtreeNodeItemToBytes(btreeNodePage.Items, btreeNodePage.PageHeader.ItemsLength)
	iStart = iEnd
	iEnd = iStart + int32(len(arr))
	copy(bs[iStart:iEnd], arr)
	return bs
}

func BytesToBtreeNodePage(bs []byte) *BtreeNodePage {
	btreeNode := &BtreeNodePage{}
	iStart := int32(0)
	btreeNode.PageHeader, iStart = BytesToBtreeNodePageHeader(bs)
	btreeNode.Items = BytesToBtreeNodeItems(bs[iStart:], btreeNode.PageHeader.ItemsLength)
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
	for _, id := range bnp.PageHeader.ChildrenId {
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

func (n node) NodeToPage() *BtreeNodePage {
	btreeNodeId := n.nodeId
	itemLength := int16(len(n.items))
	childLength := int16(len(n.children))
	childrenId := make([]int32, 0, childLength)
	for i, id := range n.children {
		childrenId[i] = id.childNodeId
	}
	f := common.INT16_LEN*3 + common.INT32_LEN*(3+childLength)
	ph := NewBtreeNodePageHeader(int32(f), btreeNodeId, itemLength, childLength, childrenId)
	bi := make([]*BtreeNodeItem, 0, itemLength)
	for i, item := range n.items {
		bi[i] = item.(*BtreeNodeItem)
	}
	return NewBreeNodePage(ph, &bi)
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
