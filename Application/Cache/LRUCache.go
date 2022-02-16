package Cache

import (
	"container/list"
	"fmt"
)

type Cache struct{
	LRUlist list.List
	LRUmap map[string]*list.Element
	MaxSize int
}

type Data struct{
	key string
	value []byte
}

func (c *Cache) Init(){
	c.LRUlist.Init()
	c.LRUmap = make(map[string]*list.Element)
}

func (c *Cache) FindKey(key string) *list.Element{
	value, ok := c.LRUmap[key]
	if ok {
		return value
	} else {
		return nil
	}
}

func(c *Cache) PrintAll(){
	var i int
	i = 0
	fmt.Println("lista:")
	for e := c.LRUlist.Front(); e != nil; e = e.Next() {
		var k Data
		k = e.Value.(Data)
		fmt.Println(i,k.key,k.value)
		i++
	}
	fmt.Println("mapa:")
	for kljuc, vr := range c.LRUmap {
		fmt.Println(kljuc, vr)
	}

}

func (c *Cache) RemoveKey(key string){
	l := c.FindKey(key)
	if l!=nil{
		delete(c.LRUmap,l.Value.(Data).key)
		c.LRUlist.Remove(l)
	}
}

func (c *Cache) AddKV(key string,value []byte){
	d := Data{key,value}
	l := c.FindKey(key)
	if l == nil{ //u slucaju da kljuc ne postoji u cache

		if c.LRUlist.Len() == c.MaxSize{ //ako je cache pun oslobadjamo nastariji element
			delete(c.LRUmap,c.LRUlist.Back().Value.(Data).key)
			c.LRUlist.Remove(c.LRUlist.Back())
		}
		c.LRUlist.PushFront(d) //dodavanje kv u listu i mapu
		c.LRUmap[key]=c.LRUlist.Front()
		return
	}
	// ako kljuc ipak postoji u cache, obrisati postojeci i dodati ga ponovo
	delete(c.LRUmap,l.Value.(Data).key)
	c.LRUlist.Remove(l)

	c.LRUlist.PushFront(d)
	c.LRUmap[key]=c.LRUlist.Front()
}