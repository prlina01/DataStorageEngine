package main

import (
	"encoding/binary"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"time"
)

func CalculateM(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

func CalculateK(delta float64) uint {
	return uint(math.Ceil(math.Log(math.E / delta)))
}

func CreateHashFunctions(k uint) []hash.Hash32 {
	h := []hash.Hash32{}
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+1)))
	}
	return h
}

type CountMinSketch struct {
	k uint
	m uint
	table [][]uint
	hashFunctions []hash.Hash32
}

func (cms *CountMinSketch) intializeCMS(delta float64, epsilon float64){
	cms.m = CalculateM(epsilon)
	cms.k = CalculateK(delta)
	cms.table = make([][]uint, cms.k)
	for i := uint(0); i<cms.k; i++{
		cms.table[i] = make([]uint, cms.m)
		for j := uint(0); j<cms.m; j++{
			cms.table[i][j] = 0
		}
	}
}

func Hashing(h hash.Hash32,s string) uint32{
	_, err := h.Write([]byte(s))
	if err != nil {
		panic("greska")
	}

	return h.Sum32()
}

func (cms *CountMinSketch) addElement(element string){
	for i := int(0); i < int(cms.k); i ++{
		cms.hashFunctions[i].Reset()
		j := uint(Hashing(cms.hashFunctions[i], element)) % cms.m
		cms.table[i][j] += 1
	}
}

func (cms *CountMinSketch) getValue (element string) uint {
	var niz = make([]uint, cms.k)

	for i := int(0); i < int(cms.k); i ++{
		cms.hashFunctions[i].Reset()
		j := uint(Hashing(cms.hashFunctions[i], element)) % cms.m
		niz[i] = cms.table[i][j]
	}
	var minimum = niz[0]
	for n := int(0); n < len(niz); n++{
		if niz[n] < minimum{
			minimum = niz[n]
		}
	}
	return minimum
}

func (cms CountMinSketch) Serialize() []byte{
	var allbytes []byte
	k := make([]byte,8)
	binary.LittleEndian.PutUint64(k, uint64(cms.k))
	allbytes = append(allbytes,k...)
	m := make([]byte,8)
	binary.LittleEndian.PutUint64(m, uint64(cms.m))
	allbytes = append(allbytes,m...)
	table := cms.table
	for line := range table {
		for lm := range table[line] {
			elem := make([]byte, 8)
			binary.LittleEndian.PutUint64(elem, uint64(table[line][lm]))
			allbytes = append(allbytes, elem...)
		}
	}
	return allbytes
}