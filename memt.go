package main

import (
	"awesomeProject5/Cache"
	"awesomeProject5/SkipList"
	"awesomeProject5/Sstable"
	"awesomeProject5/WriteAheadLog"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type Config struct {
	WalSize uint64  	`yaml:"wal_size"`
	MemtableSize uint64	`yaml:"memtable_size"`
	LowWaterMark uint8  `yaml:"low_water_mark"`
	CacheSize int 		`yaml:"cache_size"`
}
type MemTable struct{
	size uint64
	data *SkipList.SkipList
	wal *WriteAheadLog.WriteAheadLog
}

func (memtable* MemTable) Init(){
	data := memtable.wal.Data
	if len(data) >= int(memtable.size){
		return
	}
	for line:=range data{
		memtable.data.InsertNode(data[line].Key,data[line].Value)
	}
}
func (memtable* MemTable) Insert (key string, value []byte){
	if !memtable.wal.AddKV(key,value){
		panic("Not written into wal! error!")
		return
	}
	memtable.data.InsertNode(key, value)
	if memtable.size == uint64(memtable.data.GetSize()){
		memtable.Flush()
		memtable.wal.LowWaterMarkRemoval()
	}
}

func (memtable* MemTable) Flush (){
	var list []WriteAheadLog.Line
	var currentNode *SkipList.SkipListNode
	currentNode = memtable.data.GetHeader()
	currentNode = currentNode.Next[0]
	for ;currentNode != nil;{
		list = append(list, currentNode.Line)
		currentNode = currentNode.Next[0]
	}
	sst := Sstable.Sstable{}
	sst.Init(list)
	memtable.data.CreateSL()
}

func (memtable* MemTable) Delete(key string){
	memtable.data.DeleteNode(key)

}

func main() {
	var config Config
	configData, err := ioutil.ReadFile("config.yml")
	if err != nil {
		log.Fatal(err)
	}
	yaml.Unmarshal(configData, &config)
	fmt.Println(config.WalSize)
	marshalled, err := yaml.Marshal(config)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(marshalled))
	var keys []string
	keys = append(keys, "dog")
	keys = append(keys, "man")
	keys = append(keys, "ggss")
	keys = append(keys, "xv")
	keys = append(keys, "zxv")
	keys = append(keys, "gd")
	keys = append(keys, "dg")
	keys = append(keys, "gasdg")
	keys = append(keys, "daszx")
	keys = append(keys, "sdds")

	values := make([][]byte, 10)
	for i := range values {
		values[i] = make([]byte, 10)
	}

	cache := Cache.Cache{MaxSize: config.CacheSize}
	cache.Init()
	wal := WriteAheadLog.WriteAheadLog{}
	wal.Init(int64(config.WalSize))
	wal.LWM = int(config.LowWaterMark)

	mt := MemTable{config.MemtableSize, SkipList.New(20, 0, 0, nil), &wal}
	mt.Init()

	for i:= range keys{
		cache.AddKV(keys[i],values[i])
		mt.Insert(keys[i],values[i])
	}
	fmt.Println(cache.FindKey("xv"))
	fmt.Println(cache.FindKey("sdds"))
	cache.RemoveKey("gasdg")
	fmt.Println(cache.FindKey("sdds"))
	cache.PrintAll()





}
