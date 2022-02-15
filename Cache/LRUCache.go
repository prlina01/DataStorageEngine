package Cache

import (
	"container/list"
	"fmt"
)

type Cache struct{
	LRUlist list.List
	MaxSize int
}

type Data struct{
	key string
	value []byte
}

func (c *Cache) Init(){
	c.LRUlist.Init()
}

func (c *Cache) FindKey(key string) *list.Element{
	for e := c.LRUlist.Front(); e != nil; e = e.Next() {
		var k Data
		//Koristiti kasnije kod raspakivanja
		k = e.Value.(Data)
		if k.key == key{
			return e
		}
	}
	return nil
}

func(c *Cache) PrintAll(){
	var i int
	i = 0
	for e := c.LRUlist.Front(); e != nil; e = e.Next() {
		var k Data
		k = e.Value.(Data)
		fmt.Println(i,k.key,k.value)
		i++
	}

}

func (c *Cache) RemoveKey(key string){
	l := c.FindKey(key)
	if l!=nil{
		c.LRUlist.Remove(l)
	}
}

func (c *Cache) AddKV(key string,value []byte){
	d := Data{key,value}
	l := c.FindKey(key)
	if l == nil{
		if c.LRUlist.Len() == c.MaxSize{
			c.LRUlist.Remove(c.LRUlist.Back())
			c.LRUlist.PushFront(d)
			return
		}
		c.LRUlist.PushFront(d)
		return
	}
	c.LRUlist.Remove(l)
	c.LRUlist.PushFront(d)
}