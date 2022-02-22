package DataStorageEngine

import (
	"KeyDataStorage/Application/Cache"
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
	var newdse DataStorageEngine
	if err != nil {
		newdse = DataStorageEngine{10,10,1,5,4,4,2,0.005,4,1000,10,nil,nil,nil}
		DSE = newdse
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


