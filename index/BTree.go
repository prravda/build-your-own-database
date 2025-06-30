package index

import (
	"bytes"
	"encoding/binary"
)

const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 1000

// B+Tree Node
// Could be dumped to the disk
type BNode []byte

type BTree struct {
	// pointer (a non-zero page number)
	root uint64

	// callbacks for managing on-disk pages
	get func(uint64) []byte // dereference a pointer
	new func([]byte) uint64 // allocate a new page
	del func(uint64) // de-allocate a page
}

// header
const (
	BNODE_NODE = 1 // internal nodes without values
	BNODE_LEAF = 2 // leaf node with values
)

// get the 0, 1 byte for checking node type(internal or leaf)
// and convert it into uint16 
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

// get the 2, 3 byte for checking the number of keys(nkeys)
// and convert it into uint16
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// pointers
func (node BNode) getPtr(idx uint16) uint64 {
	// check index boundary
	if (idx > node.nkeys()) {
		panic("index out of range")
	}
	
	pos := HEADER + 8*idx
	
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	// TODO: implemented
}

// offset list
func offsetPos(node BNode, idx uint16) uint16 {
	// check index bound
	if (idx > node.nkeys()) {
		panic("index out of range")
	}
	
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16((node[offsetPos(node, idx):]))
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	// TODO: implement
}

// key-value
func (node BNode) kvPos(idx uint16) uint16 {
	// check index boundary
	if (idx > node.nkeys()) {
		panic("index out of range")
	}
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	// check index boundary
	if (idx > node.nkeys()) {
		panic("index out of range")
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte

// node size in byte
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// returns the first kid node whose range intersects the key (kid[i] <= key)
// TODO: implement it binary search
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys:= node.nkeys()
	found := uint16(0)

	// the first key is a copy from the parent node
	// thus it's alwas less than or equal to the key
	for i := uint16(1); i < nkeys; i++ {
		compare := bytes.Compare(node.getKey(i), key)

		if compare <= 0 {
			found = i
		} 
		if compare >= 0 {
			break
		}

	}
			return found
}

func leafInsert(
	new BNode, old BNode, idx uint16,
	key []byte, val []byte,
) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1) // header setup
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}


func leafUpdate() {
// TODO: implement
}

// copy a KV into the position
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPtr(idx, ptr)
	// KVs
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)
	// the offset of the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16(len(key)+len(val)))
}

// copy multiple KVs into the position from the old node
func nodeAppendRange(
	new BNode, old BNode,
	dstNew uint16, srcOld uint16, n uint16,
)

// replace a link with one or multiple links
func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, idx uint16,
	kids ...BNode,
) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)

	for i, node := range kids {
		// idx+uint16: position
		// tree.new(node): pointer
		// node.getKey(0): key
		// nil: val
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx-1))
}

// split a oversized node into 2 
// so that the 2nd node always fits on a page
func nodeSplitIntoTwo(left BNode, right BNode, old BNode) {
	// TODO: implement this function
}

// split a node if it's too ig
// the result are 1~3 nodes
func nodeSplitIntoThree(old BNode) (uint16, [3]BNode) {
	// if old size is smaller than page size
	// no splitting is required
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}

	// might be split later
	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	// not splitted one
	right := BNode(make([]byte, BTREE_PAGE_SIZE))

	nodeSplitIntoTwo(left, right, old)

	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		// two nodes
		return 2, [3]BNode{left, right} 
	}
	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))

	nodeSplitIntoTwo(leftleft, middle, left)

	// check node size is exceeded to BTREE_PAGE_SIZE
	if (leftleft.nbytes() > BTREE_PAGE_SIZE) {
		panic("node size should be smaller than page size")
	}

	// three nodes
	return 3, [3]BNode{leftleft, middle, right} 
}

// insert a KV into a node, the result might be split
// the caller is responsible for dealloc the input node
// and splitting and allocating result nodes.
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// the result node.
	// it's allowed to be bigger than 1 page and will be split if so
	new := make(BNode, 2*BTREE_PAGE_SIZE)

	// where to insert the key?
	idx := nodeLookupLE(node, key)

	// act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key, update it.
			// leafUpdate(new, node, idx, key, val)
		} else {
			// insert it after the position.
			leafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		// internal node, insert it to a kid node
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("bad node!")
	}
	return new
}

// part of the treeInsert(): KV insertion to an internal node
func nodeInsert(
	tree *BTree, new BNode, node BNode, idx uint16,
	key []byte, val []byte,
) {
	kptr := node.getPtr(idx)
	// recursive insertion to the kid node
	knode := treeInsert(tree, tree.get(kptr), key, val)
	// split the result
	nsplit, split := nodeSplitIntoThree(knode)
	// deallocate the kid node
	tree.del(kptr)
	// update the kid links

	nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
}

func init() {
	// header(4B)
	// 8b(pointer)
	// 2b(key len)
	// 2b(val len)
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	
	// check node size is exceeded to BTREE_PAGE_SIZE
	if (node1max > BTREE_PAGE_SIZE) {
		panic("node size should be smaller than page size")
	}
}
