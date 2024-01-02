package mbt

import (
	"MEHT/util"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"strings"
	"sync"
)

type MBTNode struct {
	nodeHash []byte
	name     []byte

	parent     *MBTNode
	subNodes   []*MBTNode
	dataHashes [][]byte

	isLeaf   bool
	isDirty  bool
	bucket   []util.KVPair
	num      int // num of kvPairs in bucket
	toDelMap map[string]map[string]int

	latch       sync.RWMutex
	updateLatch sync.Mutex
}

func NewMBTNode(name []byte, subNodes []*MBTNode, dataHashes [][]byte, isLeaf bool, db *leveldb.DB, cache *lru.Cache[string, *MBTNode]) (ret *MBTNode) {
	//由于MBT结构固定，因此此函数只会在MBt初始化时被调用，因此此时dataHashes一定为空，此时nodeHash一定仅包含name
	if isLeaf {
		ret = &MBTNode{name, name, nil, subNodes, dataHashes, isLeaf, false, make([]util.KVPair, 0),
			0, make(map[string]map[string]int), sync.RWMutex{}, sync.Mutex{}}
	} else {
		ret = &MBTNode{name, name, nil, subNodes, dataHashes, isLeaf, false, nil, -1,
			make(map[string]map[string]int), sync.RWMutex{}, sync.Mutex{}}
	}
	for _, node := range ret.subNodes {
		node.parent = ret
	}
	if cache != nil {
		cache.Add(string(ret.nodeHash), ret)
	} else {
		if err := db.Put(ret.nodeHash, SerializeMBTNode(ret), nil); err != nil {
			log.Fatal("Insert MBTNode to DB error:", err)
		}
	}
	return
}

func (mbtNode *MBTNode) GetSubNode(index int, db *leveldb.DB, cache *lru.Cache[string, *MBTNode]) *MBTNode {
	mbtNode.updateLatch.Lock()
	defer mbtNode.updateLatch.Unlock()
	if mbtNode.subNodes[index] == nil {
		var node *MBTNode
		var ok bool
		if cache != nil {
			if node, ok = cache.Get(string(mbtNode.dataHashes[index])); ok {
				mbtNode.subNodes[index] = node
				node.parent = mbtNode
			}
		}
		if !ok {
			if nodeString, err := db.Get(mbtNode.dataHashes[index], nil); err == nil {
				node, _ = DeserializeMBTNode(nodeString)
				mbtNode.subNodes[index] = node
				node.parent = mbtNode
			}
		}
	}
	return mbtNode.subNodes[index]
}

func (mbtNode *MBTNode) UpdateMBTNodeHash(db *leveldb.DB, cache *lru.Cache[string, *MBTNode]) {
	if cache != nil { //删除旧值
		cache.Remove(string(mbtNode.nodeHash))
	}
	if err := db.Delete(mbtNode.nodeHash, nil); err != nil {
		fmt.Println("Error in UpdateMBTNodeHash: ", err)
	}
	nodeHash := make([]byte, len(mbtNode.name))
	copy(nodeHash, mbtNode.name)
	if mbtNode.isLeaf {
		dataHash := make([]byte, 0)
		for _, kv := range mbtNode.bucket {
			dataHash = append(dataHash, []byte(kv.GetValue())...)
		}
		dataHash_ := sha256.Sum256(dataHash)
		mbtNode.dataHashes[0] = dataHash_[:]
	}
	for _, hash := range mbtNode.dataHashes {
		nodeHash = append(nodeHash, hash...)
	}
	newHash := sha256.Sum256(nodeHash)
	mbtNode.nodeHash = newHash[:]
	if cache != nil { //存入新值
		cache.Add(string(mbtNode.nodeHash), mbtNode)
	} else {
		if err := db.Put(mbtNode.nodeHash, SerializeMBTNode(mbtNode), nil); err != nil {
			log.Fatal("Insert MBTNode to DB error:", err)
		}
	}
}

type SeMBTNode struct {
	NodeHash   []byte
	Name       []byte
	DataHashes string
	IsLeaf     bool
	Bucket     []util.SeKVPair
}

func SerializeMBTNode(node *MBTNode) []byte {
	dataHashString := ""
	if len(node.dataHashes) > 0 {
		dataHashString += hex.EncodeToString(node.dataHashes[0])
		for _, hash := range node.dataHashes[1:] {
			dataHashString += "," + hex.EncodeToString(hash)
		}
	}
	Bucket := make([]util.SeKVPair, 0)
	for _, bk := range node.bucket {
		Bucket = append(Bucket, util.SeKVPair{Key: bk.GetKey(), Value: bk.GetValue()})
	}
	seMBTNode := &SeMBTNode{node.nodeHash, node.name, dataHashString, node.isLeaf, Bucket}
	if jsonMBTNode, err := json.Marshal(seMBTNode); err != nil {
		fmt.Printf("SerializeMBTNode error: %v\n", err)
		return nil
	} else {
		return jsonMBTNode
	}
}

func DeserializeMBTNode(data []byte) (*MBTNode, error) {
	var seMBTNode SeMBTNode
	if err := json.Unmarshal(data, &seMBTNode); err != nil {
		fmt.Printf("DeserializeMBTNode error: %v\n", err)
		return nil, err
	}
	dataHashes := make([][]byte, 0)
	dataHashStrings := strings.Split(seMBTNode.DataHashes, ",")
	for i := 0; i < len(dataHashStrings); i++ {
		dataHash, _ := hex.DecodeString(dataHashStrings[i])
		dataHashes = append(dataHashes, dataHash)
	}
	bucket := make([]util.KVPair, 0)
	for _, bk := range seMBTNode.Bucket {
		bucket = append(bucket, *util.NewKVPair(bk.Key, bk.Value))
	}
	if seMBTNode.IsLeaf {
		return &MBTNode{seMBTNode.NodeHash, seMBTNode.Name, nil, nil, dataHashes, true, false,
			bucket, len(seMBTNode.Bucket), make(map[string]map[string]int), sync.RWMutex{}, sync.Mutex{}}, nil
	} else {
		return &MBTNode{seMBTNode.NodeHash, seMBTNode.Name, nil, make([]*MBTNode, len(dataHashes)), dataHashes, false,
			false, nil, len(seMBTNode.Bucket), make(map[string]map[string]int), sync.RWMutex{}, sync.Mutex{}}, nil
	}
}