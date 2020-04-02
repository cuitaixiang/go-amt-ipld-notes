package amt

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/bits"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	logging "github.com/ipfs/go-log"
	cbg "github.com/whyrusleeping/cbor-gen"
)

var log = logging.Logger("amt")

const width = 8

var MaxIndex = uint64(1 << 48) // fairly arbitrary, but I don't want to overflow/underflow in nodesForHeight

// 根
type Root struct {
	// 高度
	Height uint64
	// 数量
	Count uint64
	// 根指向的节点
	Node Node

	store cbor.IpldStore
}

// 节点
type Node struct {
	// bit map，宽度被定为8，即其实一直是一个字节，序列化不为空
	Bmap []byte
	// 子节点链接，叶子节点无子节点链接，叶子节点序列化占位
	Links []cid.Cid
	// 叶子节点值，普通子节点无，普通子节点序列化占位
	Values []*cbg.Deferred

	// 缓存链接，位置和bit map一一对应
	expLinks []cid.Cid
	// 缓存值，位置和bit map一一对应
	expVals []*cbg.Deferred
	// 缓存子节点，位置和bit map一一对应
	cache []*Node
}

// 新建一个amt的根
func NewAMT(bs cbor.IpldStore) *Root {
	return &Root{
		store: bs,
	}
}

// 加载一个存在的amt的根
func LoadAMT(ctx context.Context, bs cbor.IpldStore, c cid.Cid) (*Root, error) {
	var r Root
	// 加载根
	if err := bs.Get(ctx, c, &r); err != nil {
		return nil, err
	}

	r.store = bs

	return &r, nil
}

// 设置第i项为val
func (r *Root) Set(ctx context.Context, i uint64, val interface{}) error {
	if i >= MaxIndex {
		return fmt.Errorf("index %d is out of range for the amt", i)
	}

	// 提取原始值
	var b []byte
	if m, ok := val.(cbg.CBORMarshaler); ok {
		buf := new(bytes.Buffer)
		if err := m.MarshalCBOR(buf); err != nil {
			return err
		}
		b = buf.Bytes()
	} else {
		var err error
		b, err = cbor.DumpObject(val)
		if err != nil {
			return err
		}
	}

	// 先判断是否超过叶子节点总数，是否需要增加高度
	for i >= nodesForHeight(width, int(r.Height)+1) { // 需要增加
		if !r.Node.empty() { // root非空
			// 先把子树保存好
			if err := r.Node.Flush(ctx, r.store, int(r.Height)); err != nil {
				return err
			}

			// 保存amt根节点，获取其cid
			c, err := r.store.Put(ctx, &r.Node)
			if err != nil {
				return err
			}

			// 新root指向原来的root
			r.Node = Node{
				Bmap:  []byte{0x01},
				Links: []cid.Cid{c},
			}
		}
		// 高度加一，继续下次循环判断
		r.Height++
	}

	// 添加值
	addVal, err := r.Node.set(ctx, r.store, int(r.Height), i, &cbg.Deferred{Raw: b})
	if err != nil {
		return err
	}

	if addVal { // 如果是新增而不是替换，总数加一
		r.Count++
	}

	return nil
}

func FromArray(ctx context.Context, bs cbor.IpldStore, vals []cbg.CBORMarshaler) (cid.Cid, error) {
	r := NewAMT(bs)
	if err := r.BatchSet(ctx, vals); err != nil {
		return cid.Undef, err
	}

	return r.Flush(ctx)
}

// 批量设置
func (r *Root) BatchSet(ctx context.Context, vals []cbg.CBORMarshaler) error {
	// TODO: there are more optimized ways of doing this method
	for i, v := range vals {
		if err := r.Set(ctx, uint64(i), v); err != nil {
			return err
		}
	}
	return nil
}

// 获取第i个值
func (r *Root) Get(ctx context.Context, i uint64, out interface{}) error {
	// 不能超过amt所能承载的最大数量
	if i >= MaxIndex {
		return fmt.Errorf("index %d is out of range for the amt", i)
	}

	// 不能超过amt叶子节点数量(2^(height+1))
	if i >= nodesForHeight(width, int(r.Height+1)) {
		return &ErrNotFound{Index: i}
	}
	return r.Node.get(ctx, r.store, int(r.Height), i, out)
}

// 获取第i个值
func (n *Node) get(ctx context.Context, bs cbor.IpldStore, height int, i uint64, out interface{}) error {
	// 计算在第几个子分支（即对应第几个比特）
	subi := i / nodesForHeight(width, height)
	// 是否存在
	set, _ := n.getBit(subi)
	if !set {
		return &ErrNotFound{i}
	}
	if height == 0 { // 到了最底层，没有子节点了，值存储于叶子节点
		n.expandValues()

		// 获取值
		d := n.expVals[i]

		if um, ok := out.(cbg.CBORUnmarshaler); ok {
			return um.UnmarshalCBOR(bytes.NewReader(d.Raw))
		} else {
			return cbor.DecodeInto(d.Raw, out)
		}
	}

	// 加载子节点
	subn, err := n.loadNode(ctx, bs, subi, false)
	if err != nil {
		return err
	}

	// 从子节点中获取，高度减一，i求余数
	return subn.get(ctx, bs, height-1, i%nodesForHeight(width, height), out)
}

func (r *Root) BatchDelete(ctx context.Context, indices []uint64) error {
	// TODO: theres a faster way of doing this, but this works for now
	for _, i := range indices {
		if err := r.Delete(ctx, i); err != nil {
			return err
		}
	}

	return nil
}

func (r *Root) Delete(ctx context.Context, i uint64) error {
	if i >= MaxIndex {
		return fmt.Errorf("index %d is out of range for the amt", i)
	}
	//fmt.Printf("i: %d, h: %d, nfh: %d\n", i, r.Height, nodesForHeight(width, int(r.Height)))
	if i >= nodesForHeight(width, int(r.Height+1)) {
		return &ErrNotFound{i}
	}

	if err := r.Node.delete(ctx, r.store, int(r.Height), i); err != nil {
		return err
	}
	r.Count--

	for r.Node.Bmap[0] == 1 && r.Height > 0 {
		sub, err := r.Node.loadNode(ctx, r.store, 0, false)
		if err != nil {
			return err
		}

		r.Node = *sub
		r.Height--
	}

	return nil
}

func (n *Node) delete(ctx context.Context, bs cbor.IpldStore, height int, i uint64) error {
	subi := i / nodesForHeight(width, height)
	set, _ := n.getBit(subi)
	if !set {
		return &ErrNotFound{i}
	}
	if height == 0 {
		n.expandValues()

		n.expVals[i] = nil
		n.clearBit(i)

		return nil
	}

	subn, err := n.loadNode(ctx, bs, subi, false)
	if err != nil {
		return err
	}

	if err := subn.delete(ctx, bs, height-1, i%nodesForHeight(width, height)); err != nil {
		return err
	}

	if subn.empty() {
		n.clearBit(subi)
		n.cache[subi] = nil
		n.expLinks[subi] = cid.Undef
	}

	return nil
}

// Subtract removes all elements of 'or' from 'r'
func (r *Root) Subtract(ctx context.Context, or *Root) error {
	// TODO: as with other methods, there should be an optimized way of doing this
	return or.ForEach(ctx, func(i uint64, _ *cbg.Deferred) error {
		return r.Delete(ctx, i)
	})
}

func (r *Root) ForEach(ctx context.Context, cb func(uint64, *cbg.Deferred) error) error {
	return r.Node.forEach(ctx, r.store, int(r.Height), 0, cb)
}

func (n *Node) forEach(ctx context.Context, bs cbor.IpldStore, height int, offset uint64, cb func(uint64, *cbg.Deferred) error) error {
	if height == 0 {
		n.expandValues()

		for i, v := range n.expVals {
			if v != nil {
				if err := cb(offset+uint64(i), v); err != nil {
					return err
				}
			}
		}

		return nil
	}

	if n.cache == nil {
		n.expandLinks()
	}

	subCount := nodesForHeight(width, height)
	for i, v := range n.expLinks {
		if v != cid.Undef {
			var sub Node
			if err := bs.Get(ctx, v, &sub); err != nil {
				return err
			}

			offs := offset + (uint64(i) * subCount)
			if err := sub.forEach(ctx, bs, height-1, offs, cb); err != nil {
				return err
			}
		}
	}
	return nil
}

// 设置扩展值，将bit map与expvalues中序号对应，方便直接使用
func (n *Node) expandValues() {
	if len(n.expVals) == 0 {
		n.expVals = make([]*cbg.Deferred, width)
		for x := uint64(0); x < width; x++ {
			set, ix := n.getBit(x)
			if set {
				n.expVals[x] = n.Values[ix]
			}
		}
	}
}

// 设置第i项值
func (n *Node) set(ctx context.Context, bs cbor.IpldStore, height int, i uint64, val *cbg.Deferred) (bool, error) {
	//nfh := nodesForHeight(width, height)
	//fmt.Printf("[set] h: %d, i: %d, subi: %d\n", height, i, i/nfh)
	if height == 0 { // 在叶子节点设置值
		n.expandValues()
		// 判断是新增还是替换
		alreadySet, _ := n.getBit(i)
		// 在缓存中更新
		n.expVals[i] = val
		// 设置相应的bit map为1
		n.setBit(i)

		return !alreadySet, nil
	}

	// 和查找原理一样，一直找到叶子节点
	nfh := nodesForHeight(width, height)

	subn, err := n.loadNode(ctx, bs, i/nfh, true)
	if err != nil {
		return false, err
	}

	return subn.set(ctx, bs, height-1, i%nfh, val)
}

// 获取bit map中第i位存在（即该位为1），存在的话是第几个（通过数1的方式）
func (n *Node) getBit(i uint64) (bool, int) {
	if i > 7 {
		panic("cant deal with wider arrays yet")
	}

	if len(n.Bmap) == 0 {
		return false, 0
	}

	if n.Bmap[0]&byte(1<<i) == 0 {
		return false, 0
	}

	mask := byte((1 << i) - 1)
	return true, bits.OnesCount8(n.Bmap[0] & mask)
}

// 设置第i位为1
func (n *Node) setBit(i uint64) {
	if i > 7 {
		panic("cant deal with wider arrays yet")
	}

	if len(n.Bmap) == 0 {
		n.Bmap = []byte{0}
	}

	n.Bmap[0] = n.Bmap[0] | byte(1<<i)
}

// 清理第i位标志，将其设置为0
func (n *Node) clearBit(i uint64) {
	if i > 7 {
		panic("cant deal with wider arrays yet")
	}

	if len(n.Bmap) == 0 {
		panic("invariant violated: called clear bit on empty node")
	}

	mask := byte(0xff - (1 << i))

	n.Bmap[0] = n.Bmap[0] & mask
}

// 设置扩展链接，将bit map与explinks中序号对应，方便直接使用
func (n *Node) expandLinks() {
	n.cache = make([]*Node, width)
	n.expLinks = make([]cid.Cid, width)
	for x := uint64(0); x < width; x++ {
		// 将子节点link存放到explink中对应bit map第i位为1的位置
		set, ix := n.getBit(x)
		if set {
			n.expLinks[x] = n.Links[ix]
		}
	}
}

// 加载一个子节点
func (n *Node) loadNode(ctx context.Context, bs cbor.IpldStore, i uint64, create bool) (*Node, error) {
	if n.cache == nil {
		// 无缓存，先分配空间
		n.expandLinks()
	} else { // 优先查缓存
		if n := n.cache[i]; n != nil {
			return n, nil
		}
	}

	// 是否存在
	set, _ := n.getBit(i)

	var subn *Node
	if set { // 存在
		var sn Node
		// explinks里面可以直接按序使用，获取子节点
		if err := bs.Get(ctx, n.expLinks[i], &sn); err != nil {
			return nil, err
		}

		// 获取的节点赋值给子节点
		subn = &sn
	} else { // 不存在
		if create { // 是否创建子节点
			subn = &Node{}
			n.setBit(i)
		} else {
			return nil, fmt.Errorf("no node found at (sub)index %d", i)
		}
	}
	// 放入缓存，供以后使用
	n.cache[i] = subn

	return subn, nil
}

// 给定高度，每个bit下，子分支承载的最大数量
func nodesForHeight(width, height int) uint64 {
	val := math.Pow(float64(width), float64(height))
	if val >= float64(math.MaxUint64) {
		log.Errorf("nodesForHeight overflow! This should never happen, please report this if you see this log message")
		return math.MaxUint64
	}

	return uint64(val)
}

// 写入磁盘
func (r *Root) Flush(ctx context.Context) (cid.Cid, error) {
	// flush缓存
	if err := r.Node.Flush(ctx, r.store, int(r.Height)); err != nil {
		return cid.Undef, err
	}

	// 存放根
	return r.store.Put(ctx, r)
}

// 是否为空
func (n *Node) empty() bool {
	return len(n.Bmap) == 0 || n.Bmap[0] == 0
}

// 缓存写入磁盘（递归过程，从下往上）
func (n *Node) Flush(ctx context.Context, bs cbor.IpldStore, depth int) error {
	if depth == 0 { // 更新叶子节点
		if len(n.expVals) == 0 {
			return nil
		}
		n.Bmap = []byte{0}
		n.Values = nil
		for i := uint64(0); i < width; i++ {
			// 叶子节点只需更新bitmap和值
			v := n.expVals[i]
			if v != nil {
				n.Values = append(n.Values, v)
				n.setBit(i)
			}
		}
		return nil
	}

	// 非叶子节点，更新bitmap和links
	if len(n.expLinks) == 0 {
		// nothing to do!
		return nil
	}

	n.Bmap = []byte{0}
	n.Links = nil

	for i := uint64(0); i < width; i++ {
		subn := n.cache[i]
		if subn != nil { // 如果缓存子节点不会空，先试着更新子节点
			if err := subn.Flush(ctx, bs, depth-1); err != nil {
				return err
			}

			// 获取子节点cid
			c, err := bs.Put(ctx, subn)
			if err != nil {
				return err
			}
			// 放入缓存links
			n.expLinks[i] = c
		}

		// 更新非子节点links
		l := n.expLinks[i]
		if l != cid.Undef {
			n.Links = append(n.Links, l)
			n.setBit(i)
		}
	}

	return nil
}

type ErrNotFound struct {
	Index uint64
}

func (e ErrNotFound) Error() string {
	return fmt.Sprintf("Index %d not found in AMT", e.Index)
}

func (e ErrNotFound) NotFound() bool {
	return true
}
