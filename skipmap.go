// Package skipmap is a high-performance concurrent map based on skip list.
package skipmap

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// Int64Map represents a map based on skip list in ascending order.
type Int64Map struct {
	header *int64Node
	tail   *int64Node
	length int64
}

type int64Node struct {
	score int64
	val   unsafe.Pointer
	next  []*int64Node
	mu    sync.Mutex
	flags bitflag
}

func newInt64Node(score int64, val interface{}, level int) *int64Node {
	n := &int64Node{
		score: score,
		next:  make([]*int64Node, level),
	}
	n.storeVal(val)
	return n
}

func (n *int64Node) storeVal(val interface{}) {
	atomic.StorePointer(&n.val, unsafe.Pointer(&val))
}

func (n *int64Node) loadVal() interface{} {
	return *(*interface{})(atomic.LoadPointer(&n.val))
}

// loadNext return `n.next[i]`(atomic)
func (n *int64Node) loadNext(i int) *int64Node {
	return (*int64Node)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&n.next[i]))))
}

// storeNext same with `n.next[i] = val`(atomic)
func (n *int64Node) storeNext(i int, val *int64Node) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&n.next[i])), unsafe.Pointer(val))
}

// NewInt64 return an empty int64 skipmap.
func NewInt64() *Int64Map {
	h, t := newInt64Node(0, "", maxLevel), newInt64Node(0, "", maxLevel)
	for i := 0; i < maxLevel; i++ {
		h.next[i] = t
	}
	h.flags.SetTrue(fullyLinked)
	t.flags.SetTrue(fullyLinked)
	return &Int64Map{
		header: h,
		tail:   t,
	}
}

// findNode takes a score and two maximal-height arrays then searches exactly as in a sequential skipmap.
// The returned preds and succs always satisfy preds[i] > score > succs[i].
// (without fullpath, if find the node will return immediately)
func (s *Int64Map) findNode(score int64, preds *[maxLevel]*int64Node, succs *[maxLevel]*int64Node) *int64Node {
	x := s.header
	for i := maxLevel - 1; i >= 0; i-- {
		succ := x.loadNext(i)
		for succ != s.tail && succ.score < score {
			x = succ
			succ = x.loadNext(i)
		}
		preds[i] = x
		succs[i] = succ

		// Check if the score already in the skipmap.
		if succ != s.tail && score == succ.score {
			return succ
		}
	}
	return nil
}

// findNodeDelete takes a score and two maximal-height arrays then searches exactly as in a sequential skip-list.
// The returned preds and succs always satisfy preds[i] > score >= succs[i].
func (s *Int64Map) findNodeDelete(score int64, preds *[maxLevel]*int64Node, succs *[maxLevel]*int64Node) int {
	// lFound represents the index of the first layer at which it found a node.
	lFound, x := -1, s.header
	for i := maxLevel - 1; i >= 0; i-- {
		succ := x.loadNext(i)
		for succ != s.tail && succ.score < score {
			x = succ
			succ = x.loadNext(i)
		}
		preds[i] = x
		succs[i] = succ

		// Check if the score already in the skip list.
		if lFound == -1 && succ != s.tail && score == succ.score {
			lFound = i
		}
	}
	return lFound
}

func unlockInt64(preds [maxLevel]*int64Node, highestLevel int) {
	var prevPred *int64Node
	for i := highestLevel; i >= 0; i-- {
		if preds[i] != prevPred { // the node could be unlocked by previous loop
			preds[i].mu.Unlock()
			prevPred = preds[i]
		}
	}
}

func (s *Int64Map) Store(key int64, val interface{}) {
	level := randomLevel()
	var preds, succs [maxLevel]*int64Node
	for {
		nodeFound := s.findNode(key, &preds, &succs)
		if nodeFound != nil { // indicating the score is already in the skip-list
			if !nodeFound.flags.Get(marked) {
				// We don't need to care about whether or not the node is fully linked,
				// just replace the value.
				nodeFound.storeVal(val)
				return
			}
			// If the node is marked, represents some other thread is in the process of deleting this node,
			// we need to add this node in next loop.
			continue
		}

		// Add this node into skip list.
		var (
			highestLocked        = -1 // the highest level being locked by this process
			valid                = true
			pred, succ, prevPred *int64Node
		)
		for layer := 0; valid && layer < level; layer++ {
			pred = preds[layer]   // target node's previous node
			succ = succs[layer]   // target node's next node
			if pred != prevPred { // the node in this layer could be locked by previous loop
				pred.mu.Lock()
				highestLocked = layer
				prevPred = pred
			}
			// valid check if there is another node has inserted into the skip list in this layer during this process.
			// It is valid if:
			// 1. The previous node and next node both are not marked.
			// 2. The previous node's next node is succ in this layer.
			valid = !pred.flags.Get(marked) && !succ.flags.Get(marked) && pred.loadNext(layer) == succ
		}
		if !valid {
			unlockInt64(preds, highestLocked)
			continue
		}

		nn := newInt64Node(key, val, level)
		for layer := 0; layer < level; layer++ {
			nn.next[layer] = succs[layer]
			preds[layer].storeNext(layer, nn)
		}
		nn.flags.SetTrue(fullyLinked)
		unlockInt64(preds, highestLocked)
		atomic.AddInt64(&s.length, 1)
	}
}

func (s *Int64Map) Load(key int64) (val interface{}, ok bool) {
	x := s.header
	for i := maxLevel - 1; i >= 0; i-- {
		nex := x.loadNext(i)
		for nex != s.tail && nex.score < key {
			x = nex
			nex = x.loadNext(i)
		}

		// Check if the score already in the skip list.
		if nex != s.tail && key == nex.score {
			if nex.flags.MGet(fullyLinked|marked, fullyLinked) {
				return nex.loadVal(), true
			}
			return nil, false
		}
	}
	return nil, false
}

func (s *Int64Map) LoadAndDelete(key int64) (val interface{}, loaded bool) {
	var (
		nodeToDelete *int64Node
		isMarked     bool // represents if this operation mark the node
		topLayer     = -1
		preds, succs [maxLevel]*int64Node
	)
	for {
		lFound := s.findNodeDelete(key, &preds, &succs)
		if isMarked || // this process mark this node or we can find this node in the skip list
			lFound != -1 && succs[lFound].flags.MGet(fullyLinked|marked, fullyLinked) && (len(succs[lFound].next)-1) == lFound {
			if !isMarked { // we don't mark this node for now
				nodeToDelete = succs[lFound]
				topLayer = lFound
				nodeToDelete.mu.Lock()
				if nodeToDelete.flags.Get(marked) {
					// The node is marked by another process,
					// the physical deletion will be accomplished by another process.
					nodeToDelete.mu.Unlock()
					return // false
				}
				nodeToDelete.flags.SetTrue(marked)
				isMarked = true
			}
			// Accomplish the physical deletion.
			var (
				highestLocked        = -1 // the highest level being locked by this process
				valid                = true
				pred, succ, prevPred *int64Node
			)
			for layer := 0; valid && (layer <= topLayer); layer++ {
				pred, succ = preds[layer], succs[layer]
				if pred != prevPred { // the node in this layer could be locked by previous loop
					pred.mu.Lock()
					highestLocked = layer
					prevPred = pred
				}
				// valid check if there is another node has inserted into the skip list in this layer
				// during this process, or the previous is deleted by another process.
				// It is valid if:
				// 1. the previous node exists.
				// 2. no another node has inserted into the skip list in this layer.
				valid = !pred.flags.Get(marked) && pred.loadNext(layer) == succ
			}
			if !valid {
				unlockInt64(preds, highestLocked)
				continue
			}
			for i := topLayer; i >= 0; i-- {
				// Now we own the `nodeToDelete`, no other goroutine will modify it.
				// So we don't need `nodeToDelete.loadNext`
				preds[i].storeNext(i, nodeToDelete.next[i])
			}
			nodeToDelete.mu.Unlock()
			unlockInt64(preds, highestLocked)
			atomic.AddInt64(&s.length, -1)
			return nodeToDelete.loadVal(), true
		}
		return // false
	}
}

func (s *Int64Map) LoadOrStore(key int64, val interface{}) (actual interface{}, loaded bool) {
	val, ok := s.Load(key)
	if !ok {
		s.Store(key, val)
		return nil, false
	}
	return val, true
}

func (s *Int64Map) Delete(key int64) {
	var (
		nodeToDelete *int64Node
		isMarked     bool // represents if this operation mark the node
		topLayer     = -1
		preds, succs [maxLevel]*int64Node
	)
	for {
		lFound := s.findNodeDelete(key, &preds, &succs)
		if isMarked || // this process mark this node or we can find this node in the skip list
			lFound != -1 && succs[lFound].flags.MGet(fullyLinked|marked, fullyLinked) && (len(succs[lFound].next)-1) == lFound {
			if !isMarked { // we don't mark this node for now
				nodeToDelete = succs[lFound]
				topLayer = lFound
				nodeToDelete.mu.Lock()
				if nodeToDelete.flags.Get(marked) {
					// The node is marked by another process,
					// the physical deletion will be accomplished by another process.
					nodeToDelete.mu.Unlock()
					return // false
				}
				nodeToDelete.flags.SetTrue(marked)
				isMarked = true
			}
			// Accomplish the physical deletion.
			var (
				highestLocked        = -1 // the highest level being locked by this process
				valid                = true
				pred, succ, prevPred *int64Node
			)
			for layer := 0; valid && (layer <= topLayer); layer++ {
				pred, succ = preds[layer], succs[layer]
				if pred != prevPred { // the node in this layer could be locked by previous loop
					pred.mu.Lock()
					highestLocked = layer
					prevPred = pred
				}
				// valid check if there is another node has inserted into the skip list in this layer
				// during this process, or the previous is deleted by another process.
				// It is valid if:
				// 1. the previous node exists.
				// 2. no another node has inserted into the skip list in this layer.
				valid = !pred.flags.Get(marked) && pred.loadNext(layer) == succ
			}
			if !valid {
				unlockInt64(preds, highestLocked)
				continue
			}
			for i := topLayer; i >= 0; i-- {
				// Now we own the `nodeToDelete`, no other goroutine will modify it.
				// So we don't need `nodeToDelete.loadNext`
				preds[i].storeNext(i, nodeToDelete.next[i])
			}
			nodeToDelete.mu.Unlock()
			unlockInt64(preds, highestLocked)
			atomic.AddInt64(&s.length, -1)
			return // true
		}
		return // false
	}
}

// Range calls f sequentially for each i and score present in the skip set.
// If f returns false, range stops the iteration.
func (s *Int64Map) Range(f func(key int64, val interface{}) bool) {
	x := s.header.loadNext(0)
	for x != s.tail {
		if !x.flags.MGet(fullyLinked|marked, fullyLinked) {
			x = x.loadNext(0)
			continue
		}
		if !f(x.score, x.loadVal()) {
			break
		}
		x = x.loadNext(0)
	}
}

// Len return the length of this skipmap.
// Keep in sync with types_gen.go:lengthFunction
// Special case for code generation, Must in the tail of skipset.go.
func (s *Int64Map) Len() int {
	return int(atomic.LoadInt64(&s.length))
}