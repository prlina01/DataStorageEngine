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
	keys = append(keys, "do4g")
	keys = append(keys, "ma7n")
	keys = append(keys, "ggss")
	keys = append(keys, "xv")
	keys = append(keys, "zxv")
	keys = append(keys, "g5d")
	keys = append(keys, "dg")
	keys = append(keys, "gas6dg")
	keys = append(keys, "da856szx")
	keys = append(keys, "das4zx2")
	keys = append(keys, "do4g4")
	keys = append(keys, "ma7n15")
	keys = append(keys, "ggss6")
	keys = append(keys, "xv42")
	keys = append(keys, "zxv35")
	keys = append(keys, "g5dgzx")
	keys = append(keys, "dghfd")
	keys = append(keys, "gas6dgvch")
	keys = append(keys, "da856szxdsh")
	keys = append(keys, "das4zxhdf")
	keys = append(keys, "do4gcxvh")
	keys = append(keys, "ma7nhdfx")
	keys = append(keys, "ggsshxdf")
	keys = append(keys, "xvhdxfh")
	keys = append(keys, "zxvhxdfhh")
	keys = append(keys, "g5dhfhfhfh")
	keys = append(keys, "dgvvvvx")
	keys = append(keys, "gas6dghfdhdfh")
	keys = append(keys, "da856szxhvhvvvh")
	keys = append(keys, "das4zxxdfhdfxdfxh")
	keys = append(keys, "do4gcxvh1")
	keys = append(keys, "ma7nhdfx2")
	keys = append(keys, "ggsshxdf4")
	keys = append(keys, "xvhdxfh5")
	keys = append(keys, "zxvhxdfh66")
	keys = append(keys, "g5dhfhfhfh6")
	keys = append(keys, "dgvvvvx6")
	keys = append(keys, "gas6dghfdhdfh72")
	keys = append(keys, "da856szxhvhvvvh11")
	keys = append(keys, "das4zxxdfhdfxdfxh33")

	values := make([][]byte, 40)
	for i := range values {
		values[i] = make([]byte, 10)
	}
	values[30][0] = 95
	values[30][1] = 94

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
	Sstable.Compaction()

	kljuc := "do4gcxvh1"
	Sstable.FindKey(kljuc)
	if mt.Data.FindElement(kljuc) == nil {
		if cache.FindKey(kljuc) == nil {
			if!(Sstable.FindKey(kljuc)) {
				fmt.Println("Key has not been found!")
			}
		}
	}


}
