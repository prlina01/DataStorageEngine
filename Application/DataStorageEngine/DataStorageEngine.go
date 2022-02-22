package DataStorageEngine

import (
	"KeyDataStorage/Application/Cache"
	"KeyDataStorage/Application/CountMinSketch"
	"KeyDataStorage/Application/HyperLogLog"
	"KeyDataStorage/Application/Memtable"
	"KeyDataStorage/Application/SkipList"
	"KeyDataStorage/Application/Sstable"
	"KeyDataStorage/Application/TokenBucket"
	"KeyDataStorage/Application/WriteAheadLog"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type DataStorageEngine struct{
	WalSize      	uint64 `yaml:"wal_size"`
	MemtableSize 	uint64 `yaml:"memtable_size"`
	LowWaterMark 	uint8 `yaml:"low_water_mark"`
	CacheSize       int `yaml:"cache_size"`
	MaxLsmTreeLevel int `yaml:"max_lsm_tree_level"`
	MaxLsmNodesFirstLevel int `yaml:"max_lsm_nodes_first_level"`
	MaxLsmNodesOtherLevels int `yaml:"max_lsm_nodes_other_levels"`
	FalsePRate   float64	`yaml:"false_positive_rate"`
	HLLPrecision uint8	`yaml:"hll_precision"`
	MaxTBSize int `yaml:"max_tokens"`
	Interval int64 `yaml:"token_bucket_interval"`
	CMSDelta float64	`yaml:"cms_delta"`
	CMSEpsilon float64	`yaml:"cms_epsilon"`
	cache *Cache.Cache
	memtable *Memtable.MemTable
	tokenbucket *TokenBucket.TokenBucket
}

func (DSE DataStorageEngine) Init() DataStorageEngine{
	configData, err := ioutil.ReadFile("config.yml")
	if err != nil {
		log.Fatal(err)
	}
	err = yaml.Unmarshal(configData, &DSE)
	if DSE.WalSize <= 0{
		DSE.WalSize = 10
	}
	if DSE.MemtableSize <= 0{
		DSE.MemtableSize = 10
	}
	if DSE.LowWaterMark <= 0{
		DSE.LowWaterMark = 1
	}
	if DSE.CacheSize <= 0{
		DSE.CacheSize = 5
	}
	if DSE.MaxLsmTreeLevel <= 0{
		DSE.MaxLsmTreeLevel = 4
	}
	if DSE.MaxLsmNodesFirstLevel <= 0{
		DSE.MaxLsmNodesFirstLevel = 4
	}
	if DSE.MaxLsmNodesOtherLevels <= 0{
		DSE.MaxLsmNodesOtherLevels = 2
	}
	if DSE.FalsePRate <= 0{
		DSE.FalsePRate = 0.05
	}
	if DSE.MaxTBSize <= 0{
		DSE.MaxTBSize = 10
	}
	if DSE.Interval <= 0{
		DSE.Interval = 15
	}
	if DSE.CMSDelta <= 0{
		DSE.CMSDelta = 0.01
	}
	if DSE.CMSEpsilon <= 0{
		DSE.CMSEpsilon = 0.01
	}

	var newdse DataStorageEngine
	if err != nil {
		newdse = DataStorageEngine{
			WalSize: 				10,
			MemtableSize:           10,
			LowWaterMark:           1,
			CacheSize:              5,
			MaxLsmTreeLevel:        4,
			MaxLsmNodesFirstLevel:  4,
			MaxLsmNodesOtherLevels: 2,
			FalsePRate:             0.005,
			HLLPrecision:           4,
			MaxTBSize:              1000,
			Interval:               10,
			CMSDelta:               0.01,
			CMSEpsilon:             0.01}
		DSE = newdse
	}
	if DSE.CMSEpsilon < 0.001 || DSE.CMSEpsilon > 0.1{
		DSE.CMSEpsilon = 0.01
	}
	if DSE.CMSDelta < 0.001 || DSE.CMSDelta > 0.1{
		DSE.CMSDelta = 0.01
	}
	if DSE.HLLPrecision < 4 || DSE.HLLPrecision > 16{
		DSE.HLLPrecision = 4
	}
	tokenbucket := TokenBucket.TokenBucket{}
	tokenbucket.Init(DSE.MaxTBSize, int(DSE.Interval))

	cache := Cache.Cache{MaxSize: DSE.CacheSize}
	cache.Init()
	wal := WriteAheadLog.WriteAheadLog{}
	wal.Init(int64(DSE.WalSize))
	wal.LWM = int(DSE.LowWaterMark)

	mt := Memtable.MemTable{
		Size: 					DSE.MemtableSize,
		Data:                   SkipList.New(20, 0, 0, nil),
		Wal:                    &wal,
		MaxLsmTreeLevel:        DSE.MaxLsmTreeLevel,
		MaxLsmNodesFirstLevel:  DSE.MaxLsmNodesFirstLevel,
		MaxLsmNodesOtherLevels: DSE.MaxLsmNodesOtherLevels,
		FalsePRate:             DSE.FalsePRate,
		HLLPrecision:           DSE.HLLPrecision}
	mt.Init()

	DSE.memtable = &mt
	DSE.cache = &cache
	DSE.tokenbucket = &tokenbucket
	return DSE
}

func (DSE *DataStorageEngine) get_without_cache(key string) []byte{
	step1 := DSE.memtable.Data.FindElement(key)
	if step1 == nil {
		step2:= DSE.cache.FindKey(key)
		if step2 == nil {
			step3 := Sstable.FindKey(key)
			if step3 == nil{
				return nil
			} else{
				return step3
			}
		}else{
			var k Cache.Data
			k = step2.Value.(Cache.Data)
			return k.Value
		}
	}else{
		if step1.Line.Tombstone == 1{
			return nil
		}
		return step1.Line.Value
	}
}
func (DSE *DataStorageEngine) GET(key string) []byte{
	ret:= DSE.get_without_cache(key)
	if ret != nil{
		DSE.cache.AddKV(key,ret)
		return ret
	}
	return nil




}
func (DSE *DataStorageEngine) SET(key string,value []byte){
	if !DSE.tokenbucket.UpdateTB(){
		fmt.Println("Premasili ste broj tokena u toku vremena")
		return
	}
	DSE.memtable.Insert(key,value)
}
func (DSE *DataStorageEngine) DELETE(key string){
	pair := DSE.get_without_cache(key)
	if pair != nil{
		DSE.memtable.Delete(key,pair)
		fmt.Print("Deleted ")
		fmt.Println(key)
	}
}

func (DSE DataStorageEngine) PUTHLL(keyhll string,data []string) {
	hllbytes := DSE.GET(keyhll)
	var HLL HyperLogLog.HLL
	var bytes []byte
	if hllbytes == nil{
		HLL = HyperLogLog.HLL{}
		HLL.Create_array(DSE.HLLPrecision)
		for line := range data {
			HLL.Add_element(data[line])
		}
		bytes = HLL.Serialize()
		DSE.SET(keyhll,bytes)
	}else{
		HLL = HyperLogLog.ParseHLL(hllbytes)
		for line := range data {
			HLL.Add_element(data[line])
		}
		bytes = HLL.Serialize()
		DSE.SET(keyhll,bytes)
	}

}

func (DSE DataStorageEngine) PUTCMS(keycms string,data []string) {
	hllbytes := DSE.GET(keycms)
	var CMS CountMinSketch.CountMinSketch
	var bytes []byte
	if hllbytes == nil{
		CMS = CountMinSketch.CountMinSketch{}
		CMS.IntializeCMS(DSE.CMSDelta,DSE.CMSEpsilon)
		CMS.HashFunctions, _ = CountMinSketch.CreateHashFunctions(CMS.K)
		for line := range data {
			CMS.AddElement(data[line])
		}
		bytes = CMS.Serialize()
		DSE.SET(keycms,bytes)
	}else{
		CMS = CountMinSketch.ParseCMS(hllbytes)
		for line := range data {
			CMS.AddElement(data[line])
		}
		bytes = CMS.Serialize()
		DSE.SET(keycms,bytes)
	}

}


