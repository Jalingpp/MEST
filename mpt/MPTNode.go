package mpt

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
)

type FullNode struct {
	nodeHash     []byte         //当前节点的哈希值,由childrenHash计算得到
	children     [16]*ShortNode // children的指针，0-15表示0-9,a-f
	childrenHash [16][]byte     // children的哈希值
	value        []byte         // 以前面ExtensionNode的prefix+suffix为key的value
}

func (fn *FullNode) GetChildInFullNode(index int, db *leveldb.DB) *ShortNode {
	//如果当前节点的children[index]为nil，则从数据库中查询
	if fn.GetChildren()[index] == nil {
		childstring, _ := db.Get(fn.GetChildrenHash()[index], nil)
		if len(childstring) != 0 {
			child, _ := DeserializeShortNode(childstring)
			fn.SetChild(index, child)
		}
	}
	return fn.GetChildren()[index]
}

type ShortNode struct {
	nodeHash []byte //当前节点的哈希值，由prefix+suffix+value/nextNodeHash计算得到

	prefix []byte //前缀

	isLeaf       bool      //是否是叶子节点
	suffix       []byte    //后缀，shared nibble（extension node）或key-end（leaf node）
	nextNode     *FullNode //下一个FullNode节点(当前节点是extension node时)
	nextNodeHash []byte    //下一个FullNode节点的哈希值
	value        []byte    //value（当前节点是leaf node时）
}

func (sn *ShortNode) GetNextNode(db *leveldb.DB) *FullNode {
	//如果当前节点的nextNode为nil，则从数据库中查询
	if sn.nextNode == nil && sn.nextNodeHash != nil {
		fmt.Printf("Find from DB")
		nextNodeString, error := db.Get(sn.nextNodeHash, nil)
		if error == nil {
			nextNode, _ := DeserializeFullNode(nextNodeString)
			sn.nextNode = nextNode
		}
	}
	return sn.nextNode
}

// NewShortNode creates a ShortNode and computes its nodeHash
func NewShortNode(prefix []byte, isLeaf bool, suffix []byte, nextNode *FullNode, value []byte, db *leveldb.DB) *ShortNode {
	nodeHash := append(prefix, suffix...)
	var nextNodeHash []byte
	if isLeaf {
		nodeHash = append(nodeHash, value...)
		nextNodeHash = nil
	} else {
		nodeHash = append(nodeHash, nextNode.nodeHash...)
		nextNodeHash = nextNode.nodeHash
	}
	hash := sha256.Sum256(nodeHash)
	nodeHash = hash[:]
	sn := &ShortNode{nodeHash, prefix, isLeaf, suffix, nextNode, nextNodeHash, value}
	//将sn写入db中
	ssn := SerializeShortNode(sn)
	err := db.Put(sn.nodeHash, ssn, nil)
	if err != nil {
		fmt.Println("Insert ShortNode to DB error:", err)
	} else {
		fmt.Printf("Inserted ShortNode=%x to DB, ssn=%x\n", sn.nodeHash, ssn)
	}
	return sn
}

// UpdateShortNodeHash 更新ShortNode的nodeHash
func UpdateShortNodeHash(sn *ShortNode, db *leveldb.DB) {
	//先删除db中原有节点
	err := db.Delete(sn.nodeHash, nil)
	if err != nil {
		fmt.Println("Delete ShortNode from DB error:", err)
	}
	nodeHash := append(sn.prefix, sn.suffix...)
	if sn.isLeaf {
		nodeHash = append(nodeHash, sn.value...)
	} else {
		nodeHash = append(nodeHash, sn.nextNode.nodeHash...)
	}
	hash := sha256.Sum256(nodeHash)
	sn.nodeHash = hash[:]
	ssn := SerializeShortNode(sn)
	//将更新后的sn写入db中
	err = db.Put(sn.nodeHash, ssn, nil)
	if err != nil {
		fmt.Println("Insert ShortNode to DB error:", err)
	} else {
		fmt.Printf("Inserted ShortNode=%x to DB, ssn=%x\n", sn.nodeHash, ssn)
	}
}

// NewFullNode creates a new FullNode and computes its nodeHash
func NewFullNode(children [16]*ShortNode, db *leveldb.DB) *FullNode {
	var childrenHash [16][]byte
	var nodeHash []byte
	for i := 0; i < 16; i++ {
		if children[i] == nil {
			childrenHash[i] = nil
		} else {
			childrenHash[i] = children[i].nodeHash
		}
		nodeHash = append(nodeHash, childrenHash[i]...)
	}
	hash := sha256.Sum256(nodeHash)
	nodeHash = hash[:]
	fn := &FullNode{nodeHash, children, childrenHash, nil}
	//将fn写入db中
	if db != nil {
		sfn := SerializeFullNode(fn)
		err := db.Put(fn.nodeHash, sfn, nil)
		if err != nil {
			fmt.Println("Insert FullNode to DB error:", err)
		} else {
			fmt.Printf("Inserted FullNode=%x to DB, sfn=%x\n", fn.nodeHash, sfn)
		}
	}
	return fn
}

// UpdateFullNodeHash updates the nodeHash of a FullNode
func UpdateFullNodeHash(fn *FullNode, db *leveldb.DB) {
	//先删除db中原有节点
	err := db.Delete(fn.nodeHash, nil)
	if err != nil {
		fmt.Println("Delete FullNode from DB error:", err)
	}
	var nodeHash []byte
	for i := 0; i < 16; i++ {
		nodeHash = append(nodeHash, fn.childrenHash[i]...)
	}
	nodeHash = append(nodeHash, fn.value...)
	hash := sha256.Sum256(nodeHash)
	fn.nodeHash = hash[:]
	//将更新后的fn写入db中
	sfn := SerializeFullNode(fn)
	err = db.Put(fn.nodeHash, sfn, nil)
	if err != nil {
		fmt.Println("Insert FullNode to DB error:", err)
	} else {
		fmt.Printf("Inserted FullNode=%x to DB, sfn=%x\n", fn.nodeHash, sfn)
	}
}

type SeShortNode struct {
	NodeHash     []byte //当前节点的哈希值，由prefix+suffix+value/nextNodeHash计算得到
	Prefix       []byte //前缀
	IsLeaf       bool   //是否是叶子节点
	Suffix       []byte //后缀，shared nibble（extension node）或key-end（leaf node）
	NextNodeHash []byte //下一个FullNode节点的哈希值
	Value        []byte //value（当前节点是leaf node时）
}

func SerializeShortNode(sn *ShortNode) []byte {
	ssn := &SeShortNode{sn.GetNodeHash(), sn.GetPrefix(), sn.GetIsLeaf(), sn.GetSuffix(), sn.GetNextNodeHash(), sn.GetValue()}
	jsonSSN, err := json.Marshal(ssn)
	if err != nil {
		fmt.Printf("SerializeShortNode error: %v\n", err)
		return nil
	}
	return jsonSSN
}

func DeserializeShortNode(ssnstring []byte) (*ShortNode, error) {
	var ssn SeShortNode
	err := json.Unmarshal(ssnstring, &ssn)
	if err != nil {
		fmt.Printf("DeserializeShortNode error: %v\n", err)
		return nil, err
	}
	sn := &ShortNode{nil, ssn.Prefix, ssn.IsLeaf, ssn.Suffix, nil, nil, ssn.Value}
	sn.nodeHash = ssn.NodeHash
	sn.SetNextNodeHash(ssn.NextNodeHash)
	return sn, nil
}

type SeFullNode struct {
	NodeHash     []byte     //当前节点的哈希值,由childrenHash计算得到
	ChildrenHash [16][]byte // children的哈希值
	Value        []byte     // 以前面ExtensionNode的prefix+suffix为key的value
}

func SerializeFullNode(fn *FullNode) []byte {
	sfn := &SeFullNode{fn.GetNodeHash(), fn.GetChildrenHash(), fn.GetValue()}
	jsonSFN, err := json.Marshal(sfn)
	if err != nil {
		fmt.Printf("SerializeFullNode error: %v\n", err)
		return nil
	}
	return jsonSFN
}

func DeserializeFullNode(sfnstring []byte) (*FullNode, error) {
	var sfn SeFullNode
	err := json.Unmarshal(sfnstring, &sfn)
	if err != nil {
		fmt.Printf("DeserializeFullNode error: %v\n", err)
		return nil, err
	}
	var children [16]*ShortNode
	for i := 0; i < 16; i++ {
		children[i] = nil
	}
	fn := &FullNode{sfn.NodeHash, children, sfn.ChildrenHash, sfn.Value}
	return fn, nil
}

func (sn *ShortNode) GetNodeHash() []byte {
	return sn.nodeHash
}

func (sn *ShortNode) GetPrefix() []byte {
	return sn.prefix
}

func (sn *ShortNode) GetSuffix() []byte {
	return sn.suffix
}

func (sn *ShortNode) GetValue() []byte {
	return sn.value
}

func (sn *ShortNode) GetNextNodeHash() []byte {
	return sn.nextNodeHash
}

func (sn *ShortNode) GetIsLeaf() bool {
	return sn.isLeaf
}

func (sn *ShortNode) SetNextNodeHash(nnh []byte) {
	sn.nextNodeHash = nnh
}

func (fn *FullNode) GetNodeHash() []byte {
	return fn.nodeHash
}

func (fn *FullNode) GetChildren() [16]*ShortNode {
	return fn.children
}

func (fn *FullNode) GetChildrenHash() [16][]byte {
	return fn.childrenHash
}

func (fn *FullNode) SetChild(index int, sn *ShortNode) {
	fn.children[index] = sn
}

func (fn *FullNode) GetValue() []byte {
	return fn.value
}
