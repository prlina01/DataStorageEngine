package Memtable

import (
	"KeyDataStorage/Application/SkipList"
	"KeyDataStorage/Application/Sstable"
	"KeyDataStorage/Application/WriteAheadLog"
)

type MemTable struct {
	Size uint64
	Data *SkipList.SkipList
	Wal  *WriteAheadLog.WriteAheadLog
}

func (memtable *MemTable) Init() {
	data := memtable.Wal.Data
	if len(data) >= int(memtable.Size) {
		return
	}
	for line := range data {
		memtable.Data.InsertNode(data[line].Key, data[line].Value)
	}
}
func (memtable *MemTable) Insert(key string, value []byte) {
	if !memtable.Wal.AddKV(key, value) {
		panic("Not written into wal! error!")
		return
	}
	memtable.Data.InsertNode(key, value)
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
	sst := Sstable.Sstable{}
	sst.Init(list)
	memtable.Data = SkipList.New(20, 0, 0, nil)
}

func (memtable *MemTable) Delete(key string) {
	memtable.Data.DeleteNode(key)

}
