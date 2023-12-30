package meht

import (
	"MEHT/mht"
	"MEHT/util"
	"encoding/json"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/syndtr/goleveldb/leveldb"
	"strings"
	"sync"
	"time"
)

// NewSEH(rdx int, bc int, bs int) *SEH {}:returns a new SEH
// GetBucketByKey(key string) *Bucket {}: returns the bucket with the given key
// GetValueByKey(key string) string {}: returns the value of the key-value pair with the given key
// GetProof(key string) (string, []byte, []mht.ProofPair) {}: returns the proof of the key-value pair with the given key
// Insert(kvPair util.KVPair) (*Bucket, string, []byte, [][]byte) {}: inserts the key-value pair into the SEH,返回插入的bucket指针,插入的value,segRootHash,proof
// PrintSEH() {}: 打印SEH

type SEH struct {
	gd  int // global depth, initial zero
	rdx int // rdx, initial  given

	bucketCapacity int // capacity of the bucket, initial given
	bucketSegNum   int // number of segment bits in the bucket, initial given

	ht            sync.Map // hash table of buckets
	bucketsNumber int      // number of buckets, initial zero
	latch         sync.RWMutex
	//updateLatch   sync.Mutex
}

// NewSEH returns a new SEH
func NewSEH(rdx int, bc int, bs int) *SEH {
	return &SEH{0, rdx, bc, bs, sync.Map{}, 0, sync.RWMutex{}}
}

// UpdateSEHToDB 更新SEH到db
func (seh *SEH) UpdateSEHToDB(db *leveldb.DB) {
	seSEH := SerializeSEH(seh)
	if err := db.Put([]byte("seh"), seSEH, nil); err != nil {
		panic(err)
	}
}

// GetBucket 获取bucket，如果内存中没有，从db中读取
func (seh *SEH) GetBucket(bucketKey string, db *leveldb.DB, cache *[]interface{}) *Bucket {
	//任何跳转到此处的函数都已对seh.ht添加了读锁，因此此处不必加锁
	ret_, ok := seh.ht.Load(bucketKey)
	ret := ret_.(*Bucket)
	if !ok {
		if len(bucketKey) > 0 {
			return seh.GetBucket(bucketKey[util.ComputeStrideByBase(seh.rdx):], db, cache)
		} else {
			return nil
		}
	} else if ret != dummyBucket {
		return ret
	}
	key_ := "bucket" + bucketKey
	if cache != nil {
		targetCache, _ := (*cache)[1].(*lru.Cache[string, *Bucket])
		ret, ok = targetCache.Get(key_)
	}
	if !ok {
		if bucketString, error_ := db.Get([]byte(key_), nil); error_ == nil {
			bucket, _ := DeserializeBucket(bucketString)
			ret = bucket
		}
	}
	seh.ht.Store(bucketKey, ret)
	return ret
}

// GetBucketByKey GetBucket returns the bucket with the given key
func (seh *SEH) GetBucketByKey(key string, db *leveldb.DB, cache *[]interface{}) *Bucket {
	if seh.gd == 0 {
		return seh.GetBucket("", db, cache)
	}
	var bKey string
	if len(key) >= seh.gd {
		bKey = key[len(key)-seh.gd*util.ComputeStrideByBase(seh.rdx):]
	} else {
		bKey = strings.Repeat("0", seh.gd*util.ComputeStrideByBase(seh.rdx)-len(key)) + key
	}
	//seh.updateLatch.Lock()
	//defer seh.updateLatch.Unlock()
	return seh.GetBucket(bKey, db, cache)
}

// GetGD returns the global depth of the SEH
func (seh *SEH) GetGD() int {
	return seh.gd
}

// GetHT returns the hash table of the SEH
func (seh *SEH) GetHT() *sync.Map {
	return &seh.ht
}

// GetBucketsNumber returns the number of buckets in the SEH
func (seh *SEH) GetBucketsNumber() int {
	return seh.bucketsNumber
}

// GetValueByKey returns the value of the key-value pair with the given key
func (seh *SEH) GetValueByKey(key string, db *leveldb.DB, cache *[]interface{}) string {
	//seh.latch.RLock()
	bucket := seh.GetBucketByKey(key, db, cache)
	//seh.latch.RUnlock()
	if bucket == nil {
		return ""
	}
	return bucket.GetValue(key, db, cache)
}

// GetProof returns the proof of the key-value pair with the given key
func (seh *SEH) GetProof(key string, db *leveldb.DB, cache *[]interface{}) (string, []byte, *mht.MHTProof) {
	bucket := seh.GetBucketByKey(key, db, cache)
	value, segKey, isSegExist, index := bucket.GetValueByKey(key, db, cache, false)
	segRootHash, mhtProof := bucket.GetProof(segKey, isSegExist, index, db, cache)
	return value, segRootHash, mhtProof
}

// Insert inserts the key-value pair into the SEH,返回插入的bucket指针,插入的value,segRootHash,proof
func (seh *SEH) Insert(kvPair util.KVPair, db *leveldb.DB, cache *[]interface{}) ([][]*Bucket, BucketDelegationCode, *int64, int64) {
	//判断是否为第一次插入
	if seh.bucketsNumber == 0 {
		//创建新的bucket
		bucket := NewBucket(0, seh.rdx, seh.bucketCapacity, seh.bucketSegNum)
		bucket.Insert(kvPair, db, cache)
		seh.ht.Store("", bucket)
		//更新bucket到db
		bucket.UpdateBucketToDB(db, cache)
		seh.bucketsNumber++
		buckets := make([]*Bucket, 0)
		buckets = append(buckets, bucket)
		return [][]*Bucket{buckets}, DELEGATE, nil, 0
	}
	//不是第一次插入,根据key和GD找到待插入的bucket
	seh.latch.RLock()
	bucket := seh.GetBucketByKey(kvPair.GetKey(), db, cache)
	seh.latch.RUnlock()
	if bucket.latch.TryLock() {
		seh.latch.RLock()
		bucket_ := seh.GetBucketByKey(kvPair.GetKey(), db, cache)
		seh.latch.RUnlock()
		if bucket_ != bucket {
			bucket.latch.Unlock()
			return nil, FAILED, nil, 0
		}
		var bucketSs [][]*Bucket
		// 成为被委托者，被委托者保证最多一次性将bucket更新满但不分裂，或者虽然引发桶分裂但不接受额外委托并只插入自己的
		bucket.DelegationLatch.Lock()
		if oldValKvp, ok := bucket.DelegationList[kvPair.GetKey()]; ok {
			newValKvp := util.NewKVPair(oldValKvp.GetKey(), oldValKvp.GetValue())
			newValKvp.AddValue(kvPair.GetValue())
			bucket.DelegationList[kvPair.GetKey()] = *newValKvp
		} else {
			value, _, _, _ := bucket.GetValueByKey(kvPair.GetKey(), db, cache, true) //连带旧值一并更新
			newValKvp := util.NewKVPair(kvPair.GetKey(), value)
			newValKvp.AddValue(kvPair.GetValue())
			bucket.DelegationList[kvPair.GetKey()] = *newValKvp
		}
		bucket.latchTimestamp = time.Now().Unix()
		bucket.DelegationLatch.Unlock() // 允许其他线程委托自己插入
		//mgtLatch.Lock()                 // 阻塞一直等到获得mgt锁，用以一次性更新并更改mgt树
		//bucket.RootLatchGainFlag = true      // 告知其他线程准备开始整体更新，让其他线程不要再尝试委托自己
		time.Sleep(time.Millisecond * 50)
		bucket.DelegationLatch.Lock()        // 获得委托锁，正式拒绝所有其他线程的委托
		if bucket.number < bucket.capacity { // 由于插入数目一定不引起桶分裂，顶多插满，因此最后插完的桶就是当前桶
			for _, kvp := range bucket.DelegationList {
				bucket.Insert(kvp, db, cache)
			}
			bucket.UpdateBucketToDB(db, cache) // 更新桶
			bucketSs = [][]*Bucket{{bucket}}
		} else { // 否则一定只插入了一个，如果不是更新则引发桶的分裂
			bucketSs = bucket.Insert(kvPair, db, cache)
			if len(bucketSs[0]) == 1 {
				bucketSs[0][0].UpdateBucketToDB(db, cache) // 更新桶
				bucketSs = [][]*Bucket{{bucket}}
			} else {
				var newLd int
				ld1 := bucketSs[0][0].GetLD()
				ld2 := bucketSs[0][1].GetLD()
				if ld1 < ld2 {
					newLd = ld1 + len(bucketSs) - 1
				} else {
					newLd = ld2 + len(bucketSs) - 1
				}
				seh.latch.Lock()
				if seh.gd < newLd {
					seh.gd = newLd
				}
				seh.latch.Unlock() //不同桶对ht的修改不会产生交集，因此尝试将seh锁释放提前，让sync.map本身特性保证ht修改的并发安全
				//无论是否扩展,均需遍历buckets,更新ht,更新buckets到db
				for i, buckets := range bucketSs {
					var bKey string
					for j := range buckets {
						if i != 0 && j == 0 { // 第一层往后每一层的第一个桶都是上一层分裂的那个桶，而上一层甚至更上层已经加过了，因此跳过
							continue
						}
						bKey = util.IntArrayToString(buckets[j].GetBucketKey(), buckets[j].rdx)
						// 在更新ht之前需要先把桶都给锁上，因为在此之后mgt还需要grow
						// 如果不锁的话，mgt在grow完成前可能桶就已经通过ht被找到，然后进行桶更新，就会出问题
						if buckets[j] != bucket { //将新分裂出来的桶给锁上，bucket本身已经被锁上了，因此不需要重复上锁
							buckets[j].latch.Lock()
						}
						seh.ht.Store(bKey, buckets[j])
						buckets[j].UpdateBucketToDB(db, cache)
					}
					toDelKey := bKey[util.ComputeStrideByBase(buckets[0].rdx):]
					seh.ht.Delete(toDelKey)
				}
				// 只在 seh 变动的位置将 seh 写入 db 可以省去很多重复写
				//seh.UpdateSEHToDB(db)
				//seh.latch.Unlock()
			}
		}
		//bucket.RootLatchGainFlag = false // 重置状态
		bucket.DelegationList = nil
		bucket.DelegationList = make(map[string]util.KVPair)
		bucket.latchTimestamp = 0
		bucket.DelegationLatch.Unlock()
		// 此处桶锁不释放，会在MGTGrow的地方
		//bucket.latch.Unlock() // 此时即使释放了桶锁也不会影响后续mgt对于根哈希的更新，因为mgt的锁还没有释放，因此当前桶不可能被任何其他线程修改
		return bucketSs, DELEGATE, nil, 0
	} else {
		// 成为委托者
		if len(bucket.DelegationList) == 0 { // 保证被委托者能第一时间拿到DelegationLatch并更新自己要插入的数据到DelegationList中
			return nil, FAILED, nil, 0
		}
		for !bucket.latch.TryLock() { // 重复查看是否存在可以委托的对象
			if len(bucket.DelegationList)+bucket.number >= bucket.capacity || bucket.number == bucket.capacity {
				// 发现一定无法再委托则退出函数并重做，直到这个桶因一个线程的插入而分裂，产生新的空间
				// 说不定重做以后这就是新的被委托者，毕竟桶已满就说明一定有一个获得了桶锁的线程在工作中
				// 而这个工作线程在不久的将来就会更新完桶并释放锁，说不定你就在上一个if代码块里工作了
				return nil, FAILED, nil, 0
			}
			if bucket.latchTimestamp == 0 { //等待一个委托线程准备好接受委托，准备好的意思就是它已经把自己要插入的数据加入到delegationList
				continue
			}
			if bucket.DelegationLatch.TryLock() {
				seh.latch.RLock()
				bucket_ := seh.GetBucketByKey(kvPair.GetKey(), db, cache)
				seh.latch.RUnlock()
				if bucket_ != bucket || len(bucket.DelegationList)+bucket.number >= bucket.capacity || bucket.number == bucket.capacity || bucket.latchTimestamp == 0 {
					// 重新检查是否可以插入，发现没位置了就只能等新一轮调整让桶分裂了
					bucket.DelegationLatch.Unlock()
					return nil, FAILED, nil, 0
				}
				if oldValKvp, ok := bucket.DelegationList[kvPair.GetKey()]; ok {
					newValKvp := util.NewKVPair(oldValKvp.GetKey(), oldValKvp.GetValue())
					newValKvp.AddValue(kvPair.GetValue())
					bucket.DelegationList[kvPair.GetKey()] = *newValKvp
				} else {
					value, _, _, _ := bucket.GetValueByKey(kvPair.GetKey(), db, cache, true) //连带旧值一并更新
					newValKvp := util.NewKVPair(kvPair.GetKey(), value)
					newValKvp.AddValue(kvPair.GetValue())
					bucket.DelegationList[kvPair.GetKey()] = *newValKvp
				}
				// 成功委托
				bucket.DelegationLatch.Unlock()
				return nil, CLIENT, &bucket.latchTimestamp, bucket.latchTimestamp
			}
		}
		// 发现没有可委托的人，重做，尝试成为被委托者
		bucket.latch.Unlock()
		return nil, FAILED, nil, 0
	}
}

// PrintSEH 打印SEH
func (seh *SEH) PrintSEH(db *leveldb.DB, cache *[]interface{}) {
	fmt.Printf("打印SEH-------------------------------------------------------------------------------------------\n")
	if seh == nil {
		return
	}
	fmt.Printf("SEH: gd=%d, rdx=%d, bucketCapacity=%d, bucketSegNum=%d, bucketsNumber=%d\n", seh.gd, seh.rdx, seh.bucketCapacity, seh.bucketSegNum, seh.bucketsNumber)
	seh.latch.RLock()
	//seh.updateLatch.Lock()
	seh.ht.Range(func(key, value interface{}) bool {
		fmt.Printf("bucketKey=%s\n", key.(string))
		seh.GetBucket(key.(string), db, cache).PrintBucket(db, cache)
		return true
	})
	//seh.updateLatch.Unlock()
	seh.latch.RUnlock()
}

type SeSEH struct {
	Gd  int // global depth, initial zero
	Rdx int // rdx, initial  given

	BucketCapacity int // capacity of the bucket, initial given
	BucketSegNum   int // number of segment bits in the bucket, initial given

	HashTableKeys []string // hash table of buckets
	BucketsNumber int      // number of buckets, initial zero
}

func SerializeSEH(seh *SEH) []byte {
	hashTableKeys := make([]string, 0)
	//seh.updateLatch.Lock()
	seh.ht.Range(func(key, value interface{}) bool {
		hashTableKeys = append(hashTableKeys, key.(string))
		return true
	})
	//seh.updateLatch.Unlock()
	seSEH := &SeSEH{seh.gd, seh.rdx, seh.bucketCapacity, seh.bucketSegNum,
		hashTableKeys, seh.bucketsNumber}
	if jsonSEH, err := json.Marshal(seSEH); err != nil {
		fmt.Printf("SerializeSEH error: %v\n", err)
		return nil
	} else {
		return jsonSEH
	}
}

func DeserializeSEH(data []byte) (*SEH, error) {
	var seSEH SeSEH
	if err := json.Unmarshal(data, &seSEH); err != nil {
		fmt.Printf("DeserializeSEH error: %v\n", err)
		return nil, err
	}
	seh := &SEH{seSEH.Gd, seSEH.Rdx, seSEH.BucketCapacity, seSEH.BucketSegNum,
		sync.Map{}, seSEH.BucketsNumber, sync.RWMutex{}}
	for _, key := range seSEH.HashTableKeys {
		if key == "" && seSEH.Gd > 0 {
			continue
		} else {
			seh.ht.Store(key, dummyBucket)
		}
	}
	return seh, nil
}
