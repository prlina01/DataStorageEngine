package DataStorageEngine

import (
	"KeyDataStorage/Application/Cache"
	"KeyDataStorage/Application/Memtable"
	"KeyDataStorage/Application/SkipList"
	"KeyDataStorage/Application/Sstable"
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
	cache *Cache.Cache
	memtable *Memtable.MemTable
}

func (DSE *DataStorageEngine) Init(){
	configData, err := ioutil.ReadFile("config.yml")
	if err != nil {
		log.Fatal(err)
	}
	err = yaml.Unmarshal(configData, &DSE)
	if err != nil {
		DSE = &DataStorageEngine{10,10,1,5,4,4,2,0.005,4,nil,nil}
	}

	cache := Cache.Cache{MaxSize: DSE.CacheSize}
	cache.Init()
	wal := WriteAheadLog.WriteAheadLog{}
	wal.Init(int64(DSE.WalSize))
	wal.LWM = int(DSE.LowWaterMark)

	mt := Memtable.MemTable{DSE.MemtableSize, SkipList.New(20, 0, 0, nil), &wal,DSE.FalsePRate,DSE.HLLPrecision}
	mt.Init()

	DSE.memtable = &mt
	DSE.cache = &cache
}

func (DSE *DataStorageEngine) GET(key string) []byte{
	step1 := DSE.memtable.Data.FindElement(key)
	if step1 == nil {
		step2:= DSE.cache.FindKey(key)
		if step2 == nil {
			step3 := Sstable.FindKey(key)
			if step3 == nil{
				return nil
			} else{
				DSE.cache.AddKV(key,step3)
				return step3
			}
		}else{
			var k Cache.Data
			k = step2.Value.(Cache.Data)
			DSE.cache.AddKV(key,k.Value)
			return k.Value
		}
	}else{
		DSE.cache.AddKV(key,step1.Line.Value)
		return step1.Line.Value
	}
}
func (DSE *DataStorageEngine) SET(key string,value []byte){
	DSE.memtable.Insert(key,value)
}
func (DSE *DataStorageEngine) DELETE(key string){
	pair := DSE.GET(key)
	if pair != nil{
		DSE.memtable.Delete(key,pair)
		fmt.Print("Deleted ")
		fmt.Println(key)
	}
}
