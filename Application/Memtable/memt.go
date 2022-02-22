package Memtable

import (
	"KeyDataStorage/Application/SkipList"
	"KeyDataStorage/Application/Sstable"
	"KeyDataStorage/Application/WriteAheadLog"
	"fmt"
)

type MemTable struct {
	Size uint64
	Data *SkipList.SkipList
	Wal  *WriteAheadLog.WriteAheadLog
	FalsePRate   float64
	HLLPrecision uint8
}

func (memtable *MemTable) Init() {
	data := memtable.Wal.Data
	if len(data) >= int(memtable.Size) {
		return
	}
	for line := range data {
		memtable.Data.InsertNode(data[line].Key, data[line].Value, int(data[line].Tombstone))
	}
}
func (memtable *MemTable) Insert(key string, value []byte) {
	if !memtable.Wal.AddKV(key, value) {
		panic("Not written into wal! error!")
		return
	}
	memtable.Data.InsertNode(key, value,0)
	if memtable.Size == uint64(memtable.Data.GetSize()) {
		memtable.Flush()
		memtable.Wal.LowWaterMarkRemoval()
	}
}

func (memtable *MemTable) Flush() {
	var list []WriteAheadLog.Line
	var currentNode *SkipList.SkipListNode
	currentNode = memtable.Data.GetHeader()
	currentNode = currentNode.Next[0]
	for currentNode != nil {
		list = append(list, currentNode.Line)
		currentNode = currentNode.Next[0]
	}
	sst := Sstable.Sstable{FalsePRate: memtable.FalsePRate, HLLPrecision:memtable.HLLPrecision}
	sst.Init(list, 1)
	memtable.Data = SkipList.New(20, 0, 0, nil)

	fmt.Println("Do you want to start compaction of LSM Tree? 1-Yes; Something else-No")
	var answer string
	_, err := fmt.Scanln(&answer)
	if err != nil {
		return
	}
	if answer == "1" {
		Sstable.Compaction()
	}
}

func (memtable *MemTable) Delete(key string,value []byte) {
	memtable.Data.DeleteNode(key,value)

}
