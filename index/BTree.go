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

// get node type and number of keys
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	// set node type
	binary.LittleEndian.PutUint16(node[0:2], btype)
	// set number of keys
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// pointers
func (node BNode) getPtr(idx uint16) uint64 {
	// check index boundary
	if (idx >= node.nkeys()) {
		panic("index out of range")
	}
	
	// the pointer size is 8 byte
	// so for getting position, multiple 8 to idx add the header size
	pos := HEADER + 8*idx
	
	// then take node after position
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	// check idx is valid 
	// Question: how can I check the idx is valid in Golang?
	if (idx >= node.nkeys()) {
		panic("index out of range")
	}

	// get position of pointers index after header size
	pos := HEADER + 8*idx

	// write val to this pos
	binary.LittleEndian.PutUint64(node[pos:], val)
}

// offset list
func offsetPos(node BNode, idx uint16) uint16 {
	// check index bound
	if idx == 0 || idx >= node.nkeys() {
		panic("index out of range")
	}
	
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	// check index bound
	if idx == 0 || idx >= node.nkeys() {
		panic("index out of range")
	}

	// Question: Is there another check logic for getOffset?
	return binary.LittleEndian.Uint16((node[offsetPos(node, idx):]))
}

func (node BNode) setOffset(idx uint16, offset uint16) {
	// get position
	pos := offsetPos(node, idx)

 // write 
 binary.LittleEndian.PutUint16(node[pos:], offset)
}

// key-value
func (node BNode) kvPos(idx uint16) uint16 {
	// check index boundary
	if (idx >= node.nkeys()) {
		panic("index out of range")
	}
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	// check index boundary
	if (idx >= node.nkeys()) {
		panic("index out of range")
	}
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

func (node BNode) getVal(idx uint16) []byte {
	// check index boundary
	if (idx >= node.nkeys()) {
		panic("index out of range")
	}

	// get position
	pos := node.kvPos(idx)

	// get keylen
	klen := binary.LittleEndian.Uint16(node[pos:])
	// and get vlen
	vlen := binary.LittleEndian.Uint16(node[pos+2:])

	return node[pos+4+klen:][:vlen]
}

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
		if compare > 0 {
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


func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	// set heaeder to new node from old one
	// mark LEAF same with old one because new one is also leaf
	// and get the nkeys for maintaining the total number of key 
	new.setHeader(BNODE_LEAF, old.nkeys())

	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
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
) {
	// if nothing to copy, just return
	if n == 0 {
		return
	}

	newStartOffset := new.getOffset((dstNew))
	oldStartPos := old.kvPos(srcOld)

	oldEndPos := old.kvPos(srcOld + n)

	bytesToCopy := oldEndPos - oldStartPos

	copy(new[new.kvPos(dstNew):new.kvPos(dstNew)+bytesToCopy], old[oldStartPos:oldEndPos])

	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+1, old.getPtr(srcOld+i))

		kvBytesWithRange := old.getOffset(srcOld + i + 1) - old.getOffset(srcOld)
		new.setOffset(dstNew+i+1, newStartOffset+kvBytesWithRange)
	}
}

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
			leafUpdate(new, node, idx, key, val)
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

// insert a new key or update an existing key
func (tree *BTree) Insert(key []byte, val []byte) {
	if tree.root == 0 {
		// create the first node with the size limit
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		// and set header to leaf node and insert two keys
		root.setHeader(BNODE_LEAF, 2)

		// a dummy key, this makes the tree cover the whole key space
		// thus, a lookup can always find a containing node
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}

	// insert via treeInsert
	node := treeInsert(tree, tree.get(tree.root), key, val)
	// try to split into three
	nsplit, split := nodeSplitIntoThree(node)
	// then delete the root
	tree.del(tree.root)

	// if nsplit is larger than 1, 
	// in other words the tree is splitted after insertion
	if nsplit > 1 {
		// add a new level
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)

		for i, keyNode := range split[:nsplit] {
			ptr, key := tree.new(keyNode), keyNode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
}

// remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	// 0. set header to old - 1
	// because a idx'th key is going to be removed
	 new.setHeader(BNODE_LEAF, old.nkeys()-1)

	 // 1. and second, move all kv pairs of old to new one except idx'th one
	 // 1-0. first, copy from 0'th to (idx-1)'th of old leaf node to new leaf node
	 nodeAppendRange(new, old, 0, 0, idx)

	 // 1-1. second, copy after idx'th 
	 // from (idx+1)'th to the end of old node
	 nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx+1))
}

// merge 2 nodes(left, right) into 1(new)
func nodeMerge(new BNode, left BNode, right BNode) {
	// 0. set header to new node 
	new.setHeader(BNODE_LEAF, left.nkeys() + right.nkeys())

	// 1. copy left's all kv to new one
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	
	// 2. copy right's all kv to new one
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}

// replace 2 adjacent links with 1
func nodeReplaceTwoKid(
	new BNode, old BNode, idx uint16, ptr uint64, key []byte,
) {
	// 0. set header to new node 
	// it is the case of new internal node, so mark it to internal node
	// and the total number of branch to leaf node, so set the total number of kv to old.nkeys()-1
	new.setHeader(BNODE_NODE, old.nkeys()-1)

	// 1. copy all kv pairs from old before first replaced link
	nodeAppendRange(new, old, 0, 0, idx)

	// 2. insert the new single merged link (pointer and key) at idx in new
	// and because this node is internal node, set value to nil
	nodeAppendKV(new, idx, ptr, key, nil)

	// 3. copy all kv pairs from old after the second replaced link
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+2))
}

// delete a key and returns whether the key was there
func (tree *BTree) Delete(key []byte) bool

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
