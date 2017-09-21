package index

import (
	"common"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"io"
	"os"
	"sort"
	"strings"
)

type Item interface {
	Less(than Item) bool
}

const (
	DefaultFreeListSize = 32
)

var (
	nilItems    = make(items, 16)
	nilChildren = make(children, 16)
)

type FreeList struct {
	freelist []*node
}

func NewFreeList(size int) *FreeList {
	return &FreeList{freelist: make([]*node, 0, size)}
}

func (f *FreeList) newNode() (n *node) {
	index := len(f.freelist) - 1
	if index < 0 {
		return new(node)
	}
	n = f.freelist[index]
	f.freelist[index] = nil
	f.freelist = f.freelist[:index]
	return
}

func (f *FreeList) freeNode(n *node) {
	if len(f.freelist) < cap(f.freelist) {
		f.freelist = append(f.freelist, n)
	}
}

// ItemIterator allows callers of Ascend* to iterate in-order over portions of
// the tree.  When this function returns false, iteration will stop and the
// associated Ascend* function will immediately return.
type ItemIterator func(i Item) bool

// New creates a new B-Tree with the given degree.
//
// New(2), for example, will create a 2-3-4 tree (each node contains 1-3 items
// and 2-4 children).
func New(degree int, path string) *BTree {
	return NewWithFreeList(degree, NewFreeList(DefaultFreeListSize), path)
}

// NewWithFreeList creates a new B-Tree that uses the given node free list.
func NewWithFreeList(degree int, f *FreeList, path string) *BTree {
	if degree <= 1 {
		panic("bad degree")
	}
	cow := copyOnWriteContext{freelist: f}
	fs, err := os.OpenFile(path, os.O_RDWR, 0666)
	cow.f = fs
	common.Check(err)
	cow.mmapmap = make(map[uint32]*mmap.MMap)
	cow.nodeIdMap = make(map[uint32]*node)
	cow.dirtyPage = make(common.Set)
	cow.mtPage = GetMetaPage(fs)
	cow.emptyList = cow.mtPage.GetEmptyList()
	cow.freelist = f
	return &BTree{
		degree: degree,
		cow:    &cow,
	}
}

// items stores items in a node.
type items []Item

// insertAt inserts a value into the given index, pushing all subsequent values
// forward.
func (s *items) insertAt(index int, item Item) {
	*s = append(*s, nil)
	if index < len(*s) {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = item
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (s *items) removeAt(index int) Item {
	item := (*s)[index]
	copy((*s)[index:], (*s)[index+1:])
	(*s)[len(*s)-1] = nil
	*s = (*s)[:len(*s)-1]
	return item
}

// pop removes and returns the last element in the list.
func (s *items) pop() (out Item) {
	index := len(*s) - 1
	out = (*s)[index]
	(*s)[index] = nil
	*s = (*s)[:index]
	return
}

// truncate truncates this instance at index so that it contains only the
// first index items. index must be less than or equal to length.
func (s *items) truncate(index int) {
	var toClear items
	*s, toClear = (*s)[:index], (*s)[index:]
	for len(toClear) > 0 {
		toClear = toClear[copy(toClear, nilItems):]
	}
}

// find returns the index where the given item should be inserted into this
// list.  'found' is true if the item already exists in the list at the given
// index.
func (s items) find(item Item) (index int, found bool) {
	i := sort.Search(len(s), func(i int) bool {
		return item.Less(s[i])
	})
	if i > 0 && !s[i-1].Less(item) {
		return i - 1, true
	}
	return i, false
}

// children stores child nodes in a node.
type childrenNode struct {
	childNode   *node
	childNodeId uint32
}
type children []*childrenNode

// insertAt inserts a value into the given index, pushing all subsequent values
// forward.
func (s *children) insertAt(index int, n *node) {
	cn := childrenNode{
		childNode:   n,
		childNodeId: n.nodeId,
	}
	*s = append(*s, nil)
	if index < len(*s) {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = &cn
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (s *children) removeAt(index int) *childrenNode {
	n := (*s)[index]
	copy((*s)[index:], (*s)[index+1:])
	(*s)[len(*s)-1] = nil
	*s = (*s)[:len(*s)-1]
	return n
}

// pop removes and returns the last element in the list.
func (s *children) pop() (out *node) {
	index := len(*s) - 1
	out = (*s)[index].childNode
	(*s)[index] = nil
	*s = (*s)[:index]
	return
}

// truncate truncates this instance at index so that it contains only the
// first index children. index must be less than or equal to length.
func (s *children) truncate(index int) {
	var toClear children
	*s, toClear = (*s)[:index], (*s)[index:]
	for len(toClear) > 0 {
		toClear = toClear[copy(toClear, nilChildren):]
	}
}

// node is an internal node in a tree.
//
// It must at all times maintain the invariant that either
//   * len(children) == 0, len(items) unconstrained
//   * len(children) == len(items) + 1
type node struct {
	nodeId   uint32
	items    items
	children children
	cow      *copyOnWriteContext
}

func (n *node) getReadableChild(i int) *node {
	if n.children[i].childNode == nil {
		return GetBTreeNodeById(n.children[i].childNodeId, n.cow)
	}
	return n.children[i].childNode
}

// split splits the given node at the given index.  The current node shrinks,
// and this function returns the item that existed at that index and a new node
// containing all items/children after it.
func (n *node) split(i int) (Item, *node) {
	item := n.items[i]
	next := n.cow.newNode()
	next.items = append(next.items, n.items[i+1:]...)
	n.items.truncate(i)
	if len(n.children) > 0 {
		next.children = append(next.children, n.children[i+1:]...)
		n.children.truncate(i + 1)
	}
	return item, next
}

// maybeSplitChild checks if a child should be split, and if so splits it.
// Returns whether or not a split occurred.
func (n *node) maybeSplitChild(i, maxItems int) bool {
	if len(n.children[i].childNode.items) < maxItems {
		return false
	}
	first := n.getReadableChild(i)
	item, second := first.split(maxItems / 2)
	n.items.insertAt(i, item)
	n.children.insertAt(i+1, second)
	n.cow.dirtyPage.Insert(n.nodeId)
	n.cow.dirtyPage.Insert(first.nodeId)
	return true
}

// insert inserts an item into the subtree rooted at this node, making sure
// no nodes in the subtree exceed maxItems items.  Should an equivalent item be
// be found/replaced by insert, it will be returned.
func (n *node) insert(item Item, maxItems int) Item {
	i, found := n.items.find(item)
	if found {
		out := n.items[i]
		n.items[i] = item
		return out
	}
	if len(n.children) == 0 {
		n.items.insertAt(i, item)
		n.cow.dirtyPage.Insert(n.nodeId)
		return nil
	}
	if n.maybeSplitChild(i, maxItems) {
		inTree := n.items[i]
		switch {
		case item.Less(inTree):
			// no change, we want first split node
		case inTree.Less(item):
			i++ // we want second split node
		default:
			n.cow.dirtyPage.Insert(n.nodeId)
			out := n.items[i]
			n.items[i] = item
			return out
		}
	}
	return n.getReadableChild(i).insert(item, maxItems)
}

// get finds the given key in the subtree and returns it.
func (n *node) get(key Item) Item {
	i, found := n.items.find(key)
	if found {
		return n.items[i]
	} else if len(n.children) > 0 {
		return n.children[i].childNode.get(key)
	}
	return nil
}

// min returns the first item in the subtree.
func min(n *node) Item {
	if n == nil {
		return nil
	}
	for len(n.children) > 0 {
		n = n.children[0].childNode
	}
	if len(n.items) == 0 {
		return nil
	}
	return n.items[0]
}

// max returns the last item in the subtree.
func max(n *node) Item {
	if n == nil {
		return nil
	}
	for len(n.children) > 0 {
		n = n.children[len(n.children)-1].childNode
	}
	if len(n.items) == 0 {
		return nil
	}
	return n.items[len(n.items)-1]
}

// toRemove details what item to remove in a node.remove call.
type toRemove int

const (
	removeItem toRemove = iota // removes the given item
	removeMin                  // removes smallest item in the subtree
	removeMax                  // removes largest item in the subtree
)

// remove removes an item from the subtree rooted at this node.
func (n *node) remove(item Item, minItems int, typ toRemove) Item {
	var i int
	var found bool
	switch typ {
	case removeMax:
		if len(n.children) == 0 {
			n.cow.dirtyPage.Insert(n.nodeId)
			return n.items.pop()
		}
		i = len(n.items)
	case removeMin:
		if len(n.children) == 0 {
			n.cow.dirtyPage.Insert(n.nodeId)
			return n.items.removeAt(0)
		}
		i = 0
	case removeItem:
		i, found = n.items.find(item)
		if len(n.children) == 0 {
			if found {
				n.cow.dirtyPage.Insert(n.nodeId)
				return n.items.removeAt(i)
			}
			return nil
		}
	default:
		panic("invalid type")
	}
	// If we get to here, we have children.
	if len(n.getReadableChild(i).items) <= minItems {
		return n.growChildAndRemove(i, item, minItems, typ)
	}
	child := n.getReadableChild(i)
	// Either we had enough items to begin with, or we've done some
	// merging/stealing, because we've got enough now and we're ready to return
	// stuff.
	if found {
		// The item exists at index 'i', and the child we've selected can give us a
		// predecessor, since if we've gotten here it's got > minItems items in it.
		out := n.items[i]
		n.cow.dirtyPage.Insert(n.nodeId)
		// We use our special-case 'remove' call with typ=maxItem to pull the
		// predecessor of item i (the rightmost leaf of our immediate left child)
		// and set it into where we pulled the item from.
		n.items[i] = child.remove(nil, minItems, removeMax)
		return out
	}
	// Final recursive call.  Once we're here, we know that the item isn't in this
	// node and that the child is big enough to remove from.
	return child.remove(item, minItems, typ)
}

// growChildAndRemove grows child 'i' to make sure it's possible to remove an
// item from it while keeping it at minItems, then calls remove to actually
// remove it.
//
// Most documentation says we have to do two sets of special casing:
//   1) item is in this node
//   2) item is in child
// In both cases, we need to handle the two subcases:
//   A) node has enough values that it can spare one
//   B) node doesn't have enough values
// For the latter, we have to check:
//   a) left sibling has node to spare
//   b) right sibling has node to spare
//   c) we must merge
// To simplify our code here, we handle cases #1 and #2 the same:
// If a node doesn't have enough items, we make sure it does (using a,b,c).
// We then simply redo our remove call, and the second time (regardless of
// whether we're in case 1 or 2), we'll have enough items and can guarantee
// that we hit case A.
func (n *node) growChildAndRemove(i int, item Item, minItems int, typ toRemove) Item {
	if i > 0 && len(n.getReadableChild(i).items) > minItems {
		// Steal from left child
		child := n.getReadableChild(i)
		stealFrom := n.getReadableChild(i - 1)
		stolenItem := stealFrom.items.pop()
		child.items.insertAt(0, n.items[i-1])
		n.items[i-1] = stolenItem
		if len(stealFrom.children) > 0 {
			child.children.insertAt(0, stealFrom.children.pop())
		}
		n.cow.dirtyPage.Insert(n.nodeId)
		n.cow.dirtyPage.Insert(child.nodeId)
		n.cow.dirtyPage.Insert(stealFrom.nodeId)
	} else if i < len(n.items) && len(n.getReadableChild(i+1).items) > minItems {
		// steal from right child
		child := n.getReadableChild(i)
		stealFrom := n.getReadableChild(i + 1)
		stolenItem := stealFrom.items.removeAt(0)
		child.items = append(child.items, n.items[i])
		n.items[i] = stolenItem
		if len(stealFrom.children) > 0 {
			child.children = append(child.children, stealFrom.children.removeAt(0))
		}
		n.cow.dirtyPage.Insert(n.nodeId)
		n.cow.dirtyPage.Insert(child.nodeId)
		n.cow.dirtyPage.Insert(stealFrom.nodeId)
	} else {
		if i >= len(n.items) {
			i--
		}
		child := n.getReadableChild(i)
		// merge with right child
		mergeItem := n.items.removeAt(i)
		mergeChild := n.children.removeAt(i + 1)
		child.items = append(child.items, mergeItem)
		child.items = append(child.items, mergeChild.childNode.items...)
		child.children = append(child.children, mergeChild.childNode.children...)
		n.cow.dirtyPage.Insert(n.nodeId)
		n.cow.dirtyPage.Insert(child.nodeId)
		n.cow.freeNode(mergeChild.childNode)
	}
	return n.remove(item, minItems, typ)
}

type direction int

const (
	descend = direction(-1)
	ascend  = direction(+1)
)

// iterate provides a simple method for iterating over elements in the tree.
//
// When ascending, the 'start' should be less than 'stop' and when descending,
// the 'start' should be greater than 'stop'. Setting 'includeStart' to true
// will force the iterator to include the first item when it equals 'start',
// thus creating a "greaterOrEqual" or "lessThanEqual" rather than just a
// "greaterThan" or "lessThan" queries.
func (n *node) iterate(dir direction, start, stop Item, includeStart bool, hit bool, iter ItemIterator) (bool, bool) {
	var ok bool
	switch dir {
	case ascend:
		for i := 0; i < len(n.items); i++ {
			if start != nil && n.items[i].Less(start) {
				continue
			}
			if len(n.children) > 0 {
				if hit, ok = n.getReadableChild(i).iterate(dir, start, stop, includeStart, hit, iter); !ok {
					return hit, false
				}
			}
			if !includeStart && !hit && start != nil && !start.Less(n.items[i]) {
				hit = true
				continue
			}
			hit = true
			if stop != nil && !n.items[i].Less(stop) {
				return hit, false
			}
			if !iter(n.items[i]) {
				return hit, false
			}
		}
		if len(n.children) > 0 {
			if hit, ok = n.getReadableChild(len(n.children)-1).iterate(dir, start, stop, includeStart, hit, iter); !ok {
				return hit, false
			}
		}
	case descend:
		for i := len(n.items) - 1; i >= 0; i-- {
			if start != nil && !n.items[i].Less(start) {
				if !includeStart || hit || start.Less(n.items[i]) {
					continue
				}
			}
			if len(n.children) > 0 {
				if hit, ok = n.getReadableChild(i+1).iterate(dir, start, stop, includeStart, hit, iter); !ok {
					return hit, false
				}
			}
			if stop != nil && !stop.Less(n.items[i]) {
				return hit, false //	continue
			}
			hit = true
			if !iter(n.items[i]) {
				return hit, false
			}
		}
		if len(n.children) > 0 {
			if hit, ok = n.getReadableChild(0).iterate(dir, start, stop, includeStart, hit, iter); !ok {
				return hit, false
			}
		}
	}
	return hit, true
}

// Used for testing/debugging purposes.
func (n *node) Print(w io.Writer, level int) {
	fmt.Fprintf(w, "%sNODE:%v\n", strings.Repeat("  ", level), n.items)
	for _, c := range n.children {
		c.childNode.Print(w, level+1)
	}
}

// BTree is an implementation of a B-Tree.
//
// BTree stores Item instances in an ordered structure, allowing easy insertion,
// removal, and iteration.
//
// Write operations are not safe for concurrent mutation by multiple
// goroutines, but Read operations are.
type BTree struct {
	degree int
	length int
	root   *node
	cow    *copyOnWriteContext
}

// copyOnWriteContext pointers determine node ownership... a tree with a write
// context equivalent to a node's write context is allowed to modify that node.
// A tree whose write context does not match a node's is not allowed to modify
// it, and must create a new, writable copy (IE: it's a Clone).
//
// When doing any write operation, we maintain the invariant that the current
// node's context is equal to the context of the tree that requested the write.
// We do this by, before we descend into any node, creating a copy with the
// correct context if the contexts don't match.
//
// Since the node we're currently visiting on any write has the requesting
// tree's context, that node is modifiable in place.  Children of that node may
// not share context, but before we descend into them, we'll make a mutable
// copy.
type copyOnWriteContext struct {
	freelist  *FreeList
	f         *os.File
	emptyList []uint32
	mmapmap   map[uint32]*mmap.MMap
	nodeIdMap map[uint32]*node
	mtPage    *MetaPage
	dirtyPage common.Set
}

// Clone clones the btree, lazily.  Clone should not be called concurrently,
// but the original tree (t) and the new tree (t2) can be used concurrently
// once the Clone call completes.
//
// The internal tree structure of b is marked read-only and shared between t and
// t2.  Writes to both t and t2 use copy-on-write logic, creating new nodes
// whenever one of b's original nodes would have been modified.  Read operations
// should have no performance degredation.  Write operations for both t and t2
// will initially experience minor slow-downs caused by additional allocs and
// copies due to the aforementioned copy-on-write logic, but should converge to
// the original performance characteristics of the original tree.
func (t *BTree) Clone() (t2 *BTree) {
	// Create two entirely new copy-on-write contexts.
	// This operation effectively creates three trees:
	//   the original, shared nodes (old b.cow)
	//   the new b.cow nodes
	//   the new out.cow nodes
	cow1, cow2 := *t.cow, *t.cow
	out := *t
	t.cow = &cow1
	out.cow = &cow2
	return &out
}

// maxItems returns the max number of items to allow per node.
func (t *BTree) maxItems() int {
	return t.degree*2 - 1
}

// minItems returns the min number of items to allow per node (ignored for the
// root node).
func (t *BTree) minItems() int {
	return t.degree - 1
}

func (c *copyOnWriteContext) newNode() (n *node) {
	n = c.freelist.newNode()
	n.cow = c
	n.nodeId = c.emptyList[0]
	_assert(len(c.emptyList) != 0, "the emptylist length is zero")
	c.emptyList = c.emptyList[1:len(c.emptyList)]
	c.nodeIdMap[n.nodeId] = n
	return
}

func (c *copyOnWriteContext) freeNode(n *node) {
	if n.cow == c {
		// clear to allow GC
		c.emptyList = append(c.emptyList, n.nodeId)
		n.items.truncate(0)
		n.children.truncate(0)
		n.cow = nil
		c.freelist.freeNode(n)
		delete(c.nodeIdMap, n.nodeId)
	}
}

// ReplaceOrInsert adds the given item to the tree.  If an item in the tree
// already equals the given one, it is removed from the tree and returned.
// Otherwise, nil is returned.
//
// nil cannot be added to the tree (will panic).
func (t *BTree) ReplaceOrInsert(item Item) Item {
	if item == nil {
		panic("nil item being added to BTree")
	}
	if t.root == nil {
		t.root = t.cow.newNode()
		t.root.items = append(t.root.items, item)
		t.length++
		return nil
	} else {
		if len(t.root.items) >= t.maxItems() {
			item2, second := t.root.split(t.maxItems() / 2)
			oldroot := t.root
			t.cow.dirtyPage.Insert(oldroot.nodeId)
			t.root = t.cow.newNode()
			t.cow.mtPage.RootId = t.root.nodeId
			t.root.items = append(t.root.items, item2)
			old := &childrenNode{childNode: oldroot, childNodeId: oldroot.nodeId}
			sec := &childrenNode{childNode: second, childNodeId: second.nodeId}
			t.root.children = append(t.root.children, old, sec)
		}
	}
	out := t.root.insert(item, t.maxItems())
	if out == nil {
		t.length++
	}
	return out
}

// Delete removes an item equal to the passed in item from the tree, returning
// it.  If no such item exists, returns nil.
func (t *BTree) Delete(item Item) Item {
	return t.deleteItem(item, removeItem)
}

// DeleteMin removes the smallest item in the tree and returns it.
// If no such item exists, returns nil.
func (t *BTree) DeleteMin() Item {
	return t.deleteItem(nil, removeMin)
}

// DeleteMax removes the largest item in the tree and returns it.
// If no such item exists, returns nil.
func (t *BTree) DeleteMax() Item {
	return t.deleteItem(nil, removeMax)
}

func (t *BTree) deleteItem(item Item, typ toRemove) Item {
	if t.root == nil || len(t.root.items) == 0 {
		return nil
	}
	out := t.root.remove(item, t.minItems(), typ)
	if len(t.root.items) == 0 && len(t.root.children) > 0 {
		oldroot := t.root
		t.cow.dirtyPage.Insert(oldroot.nodeId)
		t.root = t.root.children[0].childNode
		t.cow.mtPage.RootId = t.root.nodeId
		t.cow.freeNode(oldroot)
	}
	if out != nil {
		t.length--
	}
	return out
}

// AscendRange calls the iterator for every value in the tree within the range
// [greaterOrEqual, lessThan), until iterator returns false.
func (t *BTree) AscendRange(greaterOrEqual, lessThan Item, iterator ItemIterator) {
	if t.root == nil {
		return
	}
	t.root.iterate(ascend, greaterOrEqual, lessThan, true, false, iterator)
}

// AscendLessThan calls the iterator for every value in the tree within the range
// [first, pivot), until iterator returns false.
func (t *BTree) AscendLessThan(pivot Item, iterator ItemIterator) {
	if t.root == nil {
		return
	}
	t.root.iterate(ascend, nil, pivot, false, false, iterator)
}

// AscendGreaterOrEqual calls the iterator for every value in the tree within
// the range [pivot, last], until iterator returns false.
func (t *BTree) AscendGreaterOrEqual(pivot Item, iterator ItemIterator) {
	if t.root == nil {
		return
	}
	t.root.iterate(ascend, pivot, nil, true, false, iterator)
}

// Ascend calls the iterator for every value in the tree within the range
// [first, last], until iterator returns false.
func (t *BTree) Ascend(iterator ItemIterator) {
	if t.root == nil {
		return
	}
	t.root.iterate(ascend, nil, nil, false, false, iterator)
}

// DescendRange calls the iterator for every value in the tree within the range
// [lessOrEqual, greaterThan), until iterator returns false.
func (t *BTree) DescendRange(lessOrEqual, greaterThan Item, iterator ItemIterator) {
	if t.root == nil {
		return
	}
	t.root.iterate(descend, lessOrEqual, greaterThan, true, false, iterator)
}

// DescendLessOrEqual calls the iterator for every value in the tree within the range
// [pivot, first], until iterator returns false.
func (t *BTree) DescendLessOrEqual(pivot Item, iterator ItemIterator) {
	if t.root == nil {
		return
	}
	t.root.iterate(descend, pivot, nil, true, false, iterator)
}

// DescendGreaterThan calls the iterator for every value in the tree within
// the range (pivot, last], until iterator returns false.
func (t *BTree) DescendGreaterThan(pivot Item, iterator ItemIterator) {
	if t.root == nil {
		return
	}
	t.root.iterate(descend, nil, pivot, false, false, iterator)
}

// Descend calls the iterator for every value in the tree within the range
// [last, first], until iterator returns false.
func (t *BTree) Descend(iterator ItemIterator) {
	if t.root == nil {
		return
	}
	t.root.iterate(descend, nil, nil, false, false, iterator)
}

// Get looks for the key item in the tree, returning it.  It returns nil if
// unable to find that item.
func (t *BTree) Get(key Item) Item {
	if t.root == nil {
		return nil
	}
	return t.root.get(key)
}

// Min returns the smallest item in the tree, or nil if the tree is empty.
func (t *BTree) Min() Item {
	return min(t.root)
}

// Max returns the largest item in the tree, or nil if the tree is empty.
func (t *BTree) Max() Item {
	return max(t.root)
}

// Has returns true if the given key is in the tree.
func (t *BTree) Has(key Item) bool {
	return t.Get(key) != nil
}

// Len returns the number of items currently in the tree.
func (t *BTree) Len() int {
	return t.length
}

// Int implements the Item interface for integers.
type Int int

// Less returns true if int(a) < int(b).
func (a Int) Less(b Item) bool {
	return a < b.(Int)
}
func (a BtreeNodeItem) Less(b Item) bool {
	return a.Key < (b.(BtreeNodeItem)).Key
}

func (tr BTree) GetDirtyPage() *common.Set {
	return &tr.cow.dirtyPage
}

func (tr BTree) GetRootNode() *node {
	return tr.root
}
