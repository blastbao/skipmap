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
	score int64				// 分数
	val   unsafe.Pointer 	// 值
	next  []*int64Node		// 层
	mu    sync.Mutex		// 锁
	flags bitflag			// 标识
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
	head := newInt64Node(0, "", maxLevel)
	tail := newInt64Node(0, "", maxLevel)
	for i := 0; i < maxLevel; i++ {
		head.next[i] = tail
	}
	head.flags.SetTrue(fullyLinked)
	tail.flags.SetTrue(fullyLinked)
	return &Int64Map{
		header: head,
		tail:   tail,
	}
}

// findNode takes a score and two maximal-height arrays then searches exactly as in a sequential skipmap.
// The returned preds and succs always satisfy preds[i] > score > succs[i].
// (without fullpath, if find the node will return immediately)
func (s *Int64Map) findNode(score int64, preds *[maxLevel]*int64Node, succs *[maxLevel]*int64Node) *int64Node {

	x := s.header

	for i := maxLevel - 1; i >= 0; i-- {

		// 遍历第 i 层，直到 tail 或者找到第一个 score 大于等于 key 的节点
		next := x.loadNext(i)
		for next != s.tail && next.score < score {
			x = next
			next = x.loadNext(i)
		}

		// 如果 for 循环退出，要么已经到达 tail 要么找到首个 score 大于等于 key 的节点。
		// 此时，
		// 	x 保存了当前第 i 层目标节点的前驱节点
		//	next 保存了 `tail 节点`、或者`目标节点`、或者`目标节点的后继节点`
		preds[i] = x
		succs[i] = next

		// Check if the score already in the skipmap.
		// 如果找到了目标节点，直接返回。
		if next != s.tail && score == next.score {
			return next
		}
	}
	return nil
}

// findNodeDelete takes a score and two maximal-height arrays then searches exactly as in a sequential skip-list.
// The returned preds and succs always satisfy preds[i] > score >= succs[i].
func (s *Int64Map) findNodeDelete(score int64, preds *[maxLevel]*int64Node, succs *[maxLevel]*int64Node) int {

	// lFound represents the index of the first layer at which it found a node.
	// lFound 表示找到目标节点的首层(从上到下)标号。
	lFound := -1

	x := s.header

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

// 从 highestLevel 开始，从上到下逐层解锁
func unlockInt64(preds [maxLevel]*int64Node, highestLevel int) {
	var prevPred *int64Node
	for i := highestLevel; i >= 0; i-- {
		if preds[i] != prevPred { // the node could be unlocked by previous loop
			preds[i].mu.Unlock()
			prevPred = preds[i]
		}
	}
}

// Store sets the value for a key.
//
//
//
//
func (s *Int64Map) Store(key int64, value interface{}) {

	// 生成随机层数
	level := randomLevel()

	//
	var preds, succs [maxLevel]*int64Node
	for {

		// 查找目标节点，并保存查找链路上各层的前驱、后继节点
		nodeFound := s.findNode(key, &preds, &succs)

		// 如果找到目标节点，需要更新该节点的值
		if nodeFound != nil { // indicating the score is already in the skip-list

			// 如果节点未被标记为 "marked"，则需要更新值，更新完成后直接返回
			if !nodeFound.flags.Get(marked) {
				// We don't need to care about whether or not the node is fully linked, just replace the value.
				// 我们不需要关心这个节点是否是 "fully linked" 的，仅需替换这个值。
				nodeFound.storeVal(value)
				return
			}

			// If the node is marked, represents some other goroutines is in the process of deleting this node,
			// we need to add this node in next loop.
			//
			// 如果节点被标记为 "marked"，代表其他一些 goroutine 正在删除这个节点，则需要在下一轮循环中，再次尝试添加这个节点。
			continue
		}

		// 如果没有找到目标节点，需要添加新节点到跳表中

		var (
			highestLocked        = -1 		// the highest level being locked by this process
			valid                = true
			pred, succ, prevPred *int64Node
		)

		// 从 0 层开始，直到 level 层，逐层加锁
		for layer := 0; valid && layer < level; layer++ {

			pred = preds[layer]   // target node's previous node
			succ = succs[layer]   // target node's next node

			// 给前驱节点加锁，若不同层对应了同一个前驱节点，只需加锁一次。
			//
			// [重要] 注：
			// 在 layer 层，目标节点 target 的前驱节点为 preds[layer]
			// 按照跳表的特点，如果 preds[layer] 是紧邻 target 的前驱，那么在所有层 target 的前驱节点
			// preds[layer+1]、preds[layer+2]、... 都是同一个节点，即 preds[layer] 。
			// 所以，对这种前驱节点只需要加锁一次。

			if pred != prevPred { // the node in this layer could be locked by previous loop
				pred.mu.Lock()
				highestLocked = layer
				prevPred = pred
			}

			// valid check if there is another node has inserted into the skip list in this layer during this process.
			// It is valid if:
			// 1. The previous node and next node both are not marked.
			// 2. The previous node's next node is succ in this layer.
			//
			// 检查过程中是否有其他节点插入到本层，
			// 在以下情况下 valid 为 true :
			//	1. pred 节点和 succ 节点均未被删除：
			//	2. pred 节点的 next 节点即为 succ 节点：意味着没有新节点插入到 pred 和 succ 之间
			//
			valid = !pred.flags.Get(marked) && !succ.flags.Get(marked) && pred.loadNext(layer) == succ
		}

		// 若 valid 为 false，意味着在插入过程中，pred 和 succ 节点可能被删除了，或者有新节点插入其中间。
		// 此时，需要终止本次插入操作，释放已添加的锁，然后重新尝试插入。
		if !valid {
			unlockInt64(preds, highestLocked)
			continue
		}

		// 创建新节点
		nn := newInt64Node(key, value, level)
		// 插入新节点：逐层变更新节点的前驱后继
		for layer := 0; layer < level; layer++ {
			nn.next[layer] = succs[layer]
			preds[layer].storeNext(layer, nn)
		}
		// 更新节点状态
		nn.flags.SetTrue(fullyLinked)
		// 解锁
		unlockInt64(preds, highestLocked)
		// 增加键值对数目
		atomic.AddInt64(&s.length, 1)
	}
}

// Load returns the value stored in the map for a key, or nil if no
// value is present.
// The ok result indicates whether value was found in the map.
//
// Load 返回存储在 map 中的某个键 key 的值 value ，如果没有值，则返回 nil 。
// ok 表示是否在地图中找到了值。
func (s *Int64Map) Load(key int64) (value interface{}, ok bool) {

	// 首节点
	x := s.header

	// 从 maxLevel 层开始，逐层查找 score 等于 key 的节点。
	for i := maxLevel - 1; i >= 0; i-- {

		// 遍历第 i 层，直到 tail 或者找到第一个 score 大于等于 key 的节点
		next := x.loadNext(i)
		for next != s.tail && next.score < key {
			//[重要] x 存储查找过程中的前驱节点，它指向 score 小于目标节点 score 的最临近前驱节点，
			// 如果当前层未找到目标节点，需要继续在下一层寻找，此时需从 x 节点开始，在下一层继续查找。
			x = next
			// 向后查找
			next = x.loadNext(i)
		}

		// Check if the score already in the skip list.
		// 若 score 已存在于跳表中
		if next != s.tail && key == next.score {
			// 若 flag 为 "fullyLinked"，返回对应值
			if next.flags.MGet(fullyLinked|marked, fullyLinked) {
				return next.loadVal(), true
			}
			// 若 flag 非 "fullyLinked"，报 not exist
			return nil, false
		}

		// 至此，score 不存在于当前第 i 层中，需要继续从下一层的 x 节点处开始查找。
	}
	return nil, false
}

// LoadAndDelete deletes the value for a key, returning the previous value if any.
// The loaded result reports whether the key was present.
func (s *Int64Map) LoadAndDelete(key int64) (value interface{}, loaded bool) {

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
					return nil, false
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
		return nil, false
	}
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (s *Int64Map) LoadOrStore(key int64, value interface{}) (actual interface{}, loaded bool) {
	loadedval, ok := s.Load(key)
	if !ok {
		s.Store(key, value)
		return nil, false
	}
	return loadedval, true
}

// Delete deletes the value for a key.
func (s *Int64Map) Delete(key int64) {
	var (
		nodeToDelete *int64Node
		isMarked     bool // represents if this operation mark the node
		topLayer     = -1
		preds, succs [maxLevel]*int64Node
	)
	for {

		// 查找目标节点，若找到，返回找到的最高层号
		lFound := s.findNodeDelete(key, &preds, &succs)


		//


		if isMarked || // this process mark this node or we can find this node in the skip list
			lFound != -1 && succs[lFound].flags.MGet(fullyLinked|marked, fullyLinked) && (len(succs[lFound].next)-1) == lFound {

			//
			if !isMarked { // we don't mark this node for now

				//
				nodeToDelete = succs[lFound]
				topLayer = lFound

				//
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
			// 完成物理删除。
			var (
				highestLocked        = -1 // the highest level being locked by this process
				valid                = true
				pred, succ, prevPred *int64Node
			)

			//
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

// Range calls f sequentially for each key and val present in the skipmap.
// If f returns false, range stops the iteration.
//
// Range does not necessarily correspond to any consistent snapshot of the Map's
// contents: no key will be visited more than once, but if the value for any key
// is stored or deleted concurrently, Range may reflect any mapping for that key
// from any point during the Range call.
//
func (s *Int64Map) Range(f func(key int64, value interface{}) bool) {

	// 取 Header 的第 0 层后继节点
	x := s.header.loadNext(0)

	// 遍历到 Tail
	for x != s.tail {

		// 如果当前节点被 "marked"，则忽略
		if !x.flags.MGet(fullyLinked|marked, fullyLinked) {
			x = x.loadNext(0)
			continue
		}

		// 如果 f() 返回 false，结束遍历
		if !f(x.score, x.loadVal()) {
			break
		}

		// 取节点 x 的第 0 层后继节点
		x = x.loadNext(0)
	}
}

// Len return the length of this skipmap.
// Keep in sync with types_gen.go:lengthFunction
// Special case for code generation, Must in the tail of skipmap.go.
func (s *Int64Map) Len() int {
	return int(atomic.LoadInt64(&s.length))
}
