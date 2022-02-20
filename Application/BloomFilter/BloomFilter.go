package BloomFilter

import (
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"time"
)

type BloomFilter struct {
	M             uint
	K             uint
	HashFunctions []hash.Hash32
	BitSet        []int
	HashFunctionsConfig uint32
}

func (b1 *BloomFilter) CreateBitSet() {
	b1.BitSet = make([]int, b1.M, b1.M)
}

func (b1 *BloomFilter) AddElement(element string) {

	for j := 0; j < len(b1.HashFunctions); j++ {
		b1.HashFunctions[j].Write([]byte(element))
		i := b1.HashFunctions[j].Sum32() % uint32(b1.M)
		b1.BitSet[i] = 1
	}

}

func (b1 *BloomFilter) IsElementInBloomFilter(element string) bool {
	for j := 0; j < len(b1.HashFunctions); j++ {
		b1.HashFunctions[j].Reset()
		b1.HashFunctions[j].Write([]byte(element))
		i := b1.HashFunctions[j].Sum32() % uint32(b1.M)
		if b1.BitSet[i] == 0 {
			return false
		}
	}
	return true
}

func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func CreateHashFunctions(k uint) ([]hash.Hash32, uint32) {
	h := []hash.Hash32{}
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+i)))
	}
	return h, uint32(ts)
}
