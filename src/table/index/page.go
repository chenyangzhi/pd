package index

import (
	"common"
	"github.com/edsrzf/mmap-go"
	//logger "until/xlog4go"
	"fmt"
)

const (
	PAGESIZE          = 4096
	MMAPSIZE          = 200
	METAPAGEMAXLENGTH = 64 * PAGESIZE
	MAXPAGENUMBER     = 64*4096*8 - 4096
	INITROOTNULL      = MAXPAGENUMBER + 1
	DEGREE            = 180
	MAGIC             = 0x0dd6cfbb
	CRCSIZE           = 2
	MAXKEYSIZE        = 128
	BLOCKSIZE         = 4096 * 2
)

func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

type BtreeNodePage struct {
	PageHeader *BtreeNodePageHeaderData
	Items      []*BtreeNodeItem
}

func NewBreeNodePage(p *BtreeNodePageHeaderData, i []*BtreeNodeItem) *BtreeNodePage {
	return &BtreeNodePage{
		PageHeader: p,
		Items:      i,
	}
}

func (btreeNodePage BtreeNodePage) ToBytes() []byte {
	iStart, iEnd := int32(0), int32(0)
	bs := make([]byte, BLOCKSIZE, BLOCKSIZE)
	bp := btreeNodePage.PageHeader.ToBytes()
	iEnd = iStart + int32(len(bp))
	copy(bs[iStart:iEnd], bp)
	arr := BatchBtreeNodeItemToBytes(btreeNodePage.Items)
	iStart = iEnd
	iEnd = iStart + int32(len(arr))
	copy(bs[iStart:iEnd], arr)
	return bs
}

func BytesToBtreeNodePage(bs []byte) *BtreeNodePage {
	btreeNode := &BtreeNodePage{}
	iStart := uint32(0)
	btreeNode.PageHeader, iStart = BytesToBtreeNodePageHeader(bs)
	btreeNode.Items = BytesToBtreeNodeItems(bs[iStart:], btreeNode.PageHeader.ItemsLength)
	return btreeNode
}

func GetMmapId(nodeId uint32) uint32 {
	return nodeId / MMAPSIZE
}

func GetMmapOffset(nodeId uint32) uint32 {
	return nodeId % MMAPSIZE * BLOCKSIZE
}
func GetMMapRegion(numberMmap uint32, c *copyOnWriteContext) mmap.MMap {
	if val, ok := c.mmapmap[numberMmap]; ok {
		return val
	}
	off := int64(numberMmap*MMAPSIZE*BLOCKSIZE + METAPAGEMAXLENGTH)
	mmp, err := mmap.MapRegion(c.f, BLOCKSIZE*MMAPSIZE, mmap.RDWR, 0, off)
	common.Check(err)
	c.mmapmap[numberMmap] = mmp
	return mmp
}

func (bp BtreeNodePage) WriteToMMapRegion(cow *copyOnWriteContext) {
	mId := GetMmapId(bp.PageHeader.BtreeNodeId)
	m := cow.mmapmap[mId]
	if m == nil {
		m = GetMMapRegion(mId, cow)
	}
	iStart := GetMmapOffset(bp.PageHeader.BtreeNodeId)
	iEnd := iStart + BLOCKSIZE
	copy(m[iStart:iEnd], bp.ToBytes())
	return
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
	n.items = make(items, len(bnp.Items), len(bnp.Items))
	for i, o := range bnp.Items {
		n.items[i] = o
	}
	cow.nodeIdMap[n.nodeId] = &n
	return &n
}

func (n node) NodeToPage() *BtreeNodePage {
	btreeNodeId := n.nodeId
	itemLength := uint16(len(n.items))
	childLength := uint16(len(n.children))
	childrenId := make([]uint32, childLength, childLength)
	for i, id := range n.children {
		childrenId[i] = id.childNodeId
	}
	f := common.INT16_LEN*3 + common.INT32_LEN*(3+childLength)
	ph := NewBtreeNodePageHeader(uint32(f), btreeNodeId, itemLength, childLength, childrenId)
	bi := make([]*BtreeNodeItem, itemLength, itemLength)
	for i, item := range n.items {
		bi[i] = item.(*BtreeNodeItem)
	}
	return NewBreeNodePage(ph, bi)
}

func BuildBTreeFromPage(baseTableColumn string) *BTree {
	tr := New(DEGREE, baseTableColumn)
	rootId := tr.cow.mtPage.RootId
	if rootId == INITROOTNULL {
		return tr
	}
	mmapId := GetMmapId(rootId)
	m := GetMMapRegion(mmapId, tr.cow)
	tr.cow.mmapmap[mmapId] = m
	offset := GetMmapOffset(rootId)
	p := BytesToBtreeNodePage(m[offset : offset+BLOCKSIZE])
	tr.root = PageToNode(p, tr.cow)
	return tr
}

func GetBTreeNodeById(id uint32, cow *copyOnWriteContext) *node {
	mmapId := GetMmapId(id)
	mmap := GetMMapRegion(mmapId, cow)
	iStart := id % MMAPSIZE * BLOCKSIZE
	iEnd := iStart + BLOCKSIZE
	pageNode := BytesToBtreeNodePage(mmap[iStart:iEnd])
	return PageToNode(pageNode, cow)
}
