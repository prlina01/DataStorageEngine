package CountMinSketch

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

func CreateHashFunctions(k uint) ([]hash.Hash32, uint32) {
	h := []hash.Hash32{}
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+i)))
	}
	return h, uint32(ts)
}


type CountMinSketch struct {
	K uint
	m uint
	table               [][]uint
	HashFunctions       []hash.Hash32
	HashFunctionsConfig uint32
}

func (cms *CountMinSketch) IntializeCMS(delta float64, epsilon float64){
	cms.m = CalculateM(epsilon)
	cms.K = CalculateK(delta)
	cms.table = make([][]uint, cms.K)
	for i := uint(0); i<cms.K; i++{
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

func (cms *CountMinSketch) AddElement(element string){
	for i := int(0); i < int(cms.K); i ++{
		cms.HashFunctions[i].Reset()
		j := uint(Hashing(cms.HashFunctions[i], element)) % cms.m
		cms.HashFunctions[i].Reset()
		cms.table[i][j] += 1
	}
}

func (cms *CountMinSketch) getValue (element string) uint {
	var niz = make([]uint, cms.K)

	for i := int(0); i < int(cms.K); i ++{
		cms.HashFunctions[i].Reset()
		j := uint(Hashing(cms.HashFunctions[i], element)) % cms.m
		cms.HashFunctions[i].Reset()
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
	binary.LittleEndian.PutUint64(k, uint64(cms.K))
	allbytes = append(allbytes,k...)
	m := make([]byte,8)
	binary.LittleEndian.PutUint64(m, uint64(cms.m))
	allbytes = append(allbytes,m...)
	hashConfiguration := cms.HashFunctionsConfig
	elemHash := make([]byte, 4)
	binary.LittleEndian.PutUint32(elemHash, hashConfiguration)
	allbytes = append(allbytes, elemHash...)
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

func ParseCMS(allbytes []byte) CountMinSketch {
	cms := CountMinSketch{};
	var bit_set [][]uint
	var i int
	var h []hash.Hash32
	i = 0
	mbytes := allbytes[i:8]
	i+=8
	cms.K = uint(binary.LittleEndian.Uint64(mbytes))
	kbytes := allbytes[i:16]
	i = 16
	cms.m = uint(binary.LittleEndian.Uint32(kbytes))
	config := allbytes[i:20]
	cms.HashFunctionsConfig = uint32(binary.LittleEndian.Uint32(config))
	for i := uint(0); i < cms.K; i++ {
		h = append(h, murmur3.New32WithSeed(cms.HashFunctionsConfig+uint32(i)))
	}
	i = 28
	cms.HashFunctions = h
	cms.table = make([][]uint, cms.K)
	for y := uint(0); y<cms.K; y++{
		cms.table[y] = make([]uint, cms.m)
		for j := uint(0); j<cms.m; j++{
			elem := allbytes[i-8:i]
			var intelem uint
			intelem = uint(binary.LittleEndian.Uint64(elem))
			cms.table[y][j] = intelem
		}
	}

	cms.table = bit_set

	return cms
}