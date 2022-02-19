package main

import (
	"KeyDataStorage/Application/Cache"
	"KeyDataStorage/Application/Memtable"
	"KeyDataStorage/Application/SkipList"
	"KeyDataStorage/Application/Sstable"
	"KeyDataStorage/Application/WriteAheadLog"
	"fmt"
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type Config struct {
	WalSize      uint64 `yaml:"wal_size"`
	MemtableSize uint64 `yaml:"memtable_size"`
	LowWaterMark uint8  `yaml:"low_water_mark"`
	CacheSize       int `yaml:"cache_size"`
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
	keys = append(keys, "daszx1")

	values := make([][]byte, 10)
	for i := range values {
		values[i] = make([]byte, 10)
	}

	cache := Cache.Cache{MaxSize: config.CacheSize}
	cache.Init()
	wal := WriteAheadLog.WriteAheadLog{}
	wal.Init(int64(config.WalSize))
	wal.LWM = int(config.LowWaterMark)

	mt := Memtable.MemTable{config.MemtableSize, SkipList.New(20, 0, 0, nil), &wal}
	mt.Init()

	for i := range keys {
		cache.AddKV(keys[i], values[i])
		mt.Insert(keys[i], values[i])
	}
	fmt.Println(cache.FindKey("xv"))
	fmt.Println(cache.FindKey("sdds"))
	cache.RemoveKey("gasdg")
	fmt.Println(cache.FindKey("sdds"))
	cache.PrintAll()
	Sstable.Compaction()


}
