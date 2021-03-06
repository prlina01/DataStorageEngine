package SkipList

import (
	"KeyDataStorage/Application/WriteAheadLog"
	"math/rand"
	"time"
)


type SkipList struct {
	maxHeight int
	height    int
	size      int
	head      *SkipListNode
	}

func New(maxHeight int,height int,size int,head *SkipListNode) *SkipList {
	sl := SkipList{maxHeight,height,size,head}
	sl.CreateSL()
	return &sl
}

type SkipListNode struct {
	Line WriteAheadLog.Line
	Next []*SkipListNode
}


func (s *SkipList) GetSize() int{
	return s.size
}

func (s *SkipList) GetHeader() *SkipListNode {
	return s.head
}

func (s *SkipList) CreateSL() {
	mybytes := []byte("")
	chcks := WriteAheadLog.CRC32(mybytes)

	t:= time.Now().Unix()
	fulltimestamp := uint(t)

	tombstone := byte(0)

	keysize := uint64(len(""))

	value := make([]byte,10)
	valuesize := uint64(len(value))
	var line WriteAheadLog.Line
	line.Crc = chcks
	line.Value = value
	line.Timestamp = uint64(fulltimestamp)
	line.Tombstone = tombstone
	line.Keysize = keysize
	line.Valuesize = valuesize
	line.Key = ""
	Headernode := SkipListNode{line,make([]*SkipListNode,s.maxHeight)}

	s.head = &Headernode


}
func (s *SkipList) FindElement(key string)  *SkipListNode {

	node := s.head
	for {

		for _, subNode := range (node).Next {
			if node.Line.Key == key {
				return node
			} else if subNode == nil{
				node = subNode
				break
			} else if subNode.Line.Key < key {
				node = subNode
				break
			} else if subNode.Line.Key >= key {
				node = subNode
				break
			}
		}
		if node == nil {
			return nil
		}

	}

}
func (s *SkipList) InsertNode(key string,value []byte,ts int) {
	update := make([]*SkipListNode,s.maxHeight)
	x := s.head
	for i:=s.height; i >= 0;i--{
		for ;x.Next[i]!=nil && x.Next[i].Line.Key < key;{
			x = x.Next[i]
		}
		update[i] = x
	}
	x = x.Next[0]
	if x == nil || key != x.Line.Key {
		s.size += 1
		level := s.roll()
		if level > s.height {
			for i := s.height + 1; i <= level; i++ {
				update[i] = s.head
			}
			s.height = level
		}
		mybytes := []byte(key)
		chcks := WriteAheadLog.CRC32(mybytes)
		t := time.Now().Unix()
		fulltimestamp := uint(t)
		tombstone := byte(ts)
		keysize := uint64(len(key))
		valuesize := uint64(len(value))
		var line WriteAheadLog.Line
		line.Crc = chcks
		line.Value = value
		line.Timestamp = uint64(fulltimestamp)
		line.Tombstone = tombstone
		line.Keysize = keysize
		line.Valuesize = valuesize
		line.Key = key
		n := SkipListNode{line, make([]*SkipListNode, level+1)}
		for i := 0; i <= level; i++ {
			n.Next[i] = update[i].Next[i]
			update[i].Next[i] = &n
		}
		return
		}
	if x.Line.Key == key{
		mybytes := []byte(key)
		chcks := WriteAheadLog.CRC32(mybytes)
		t:= time.Now().Unix()
		fulltimestamp := uint(t)
		tombstone := byte(ts)
		keysize := uint64(len(key))
		valuesize := uint64(len(value))
		var line WriteAheadLog.Line
		line.Crc = chcks
		line.Value = value
		line.Timestamp = uint64(fulltimestamp)
		line.Tombstone = tombstone
		line.Keysize = keysize
		line.Valuesize = valuesize
		line.Key = key
		x.Line = line
		return
	}
}
func (s *SkipList) DeleteNode(key string,value []byte) {
	update := make([]*SkipListNode, s.maxHeight)
	x := s.head
	for i := s.height; i >= 0; i-- {
		for ; x.Next[i] != nil && x.Next[i].Line.Key < key; {
			x = x.Next[i]
		}
		update[i] = x
	}
	x = x.Next[0]
	elem := s.FindElement(key)
	if x == elem && x!=nil {
		x.Line.Tombstone = 1
	}else{
		s.InsertNode(key,value,1)}
	}




func (s *SkipList) roll() int {
	level := 0
	rand.Seed(time.Now().UnixNano())
	for ; rand.Int31n(2) == 1; level++ {
		if level > s.maxHeight {
			return level
		}
	}
	return level
}
