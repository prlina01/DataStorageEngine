package HyperLogLog

import (
	"encoding/binary"
	"hash/fnv"
	"io"
	"math"
	"math/bits"
	"os"

	//"crypto/md5"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HLL struct {
	m   uint64
	p   uint8
	reg []uint8
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)),-1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}



func hashFunc(element string) uint32{
	hash := fnv.New32a()
	hash.Write([]byte(element))
	return hash.Sum32()
}



func (hll *HLL) Create_array(p uint8){
	hll.p = p
	hll.m = uint64(math.Pow(2, float64(p)))
	hll.reg = make([]uint8, hll.m)
}

func (hll *HLL) Add_element(element string){
	var k = hashFunc(element)

	var mask uint32 = 0
	for i := uint8(0); i <= hll.p; i++ {
		mask += uint32(math.Pow(2, float64(32-i)))
	}
	var bucket = k & mask
	bucket = bits.RotateLeft32(bucket, int(hll.p))
	hll.reg[bucket] = uint8(math.Max(float64(hll.reg[bucket]), float64(bits.TrailingZeros32(k))+1))

}


func (hll *HLL) Serialize() []byte{
	var allbytes []byte
	m := make([]byte,8)
	binary.LittleEndian.PutUint64(m,hll.m)
	allbytes = append(allbytes,m...)
	p := make([]byte,4)
	binary.LittleEndian.PutUint32(p, uint32(hll.p))
	allbytes = append(allbytes,p...)
	reg := hll.reg
	for line := range reg {
		elem := make([]byte, 4)
		binary.LittleEndian.PutUint32(elem, uint32(reg[line]))
		allbytes = append(allbytes, elem...)
	}
	return allbytes
}

func ParseHLL(f *os.File) HLL {
	hll := HLL{};
	var bit_set []uint8
	mbytes := make([]byte, 8)
	_, _ = f.Read(mbytes)
	hll.m = binary.LittleEndian.Uint64(mbytes)
	kbytes := make([]byte, 4)
	_, _ = f.Read(kbytes)
	hll.p = uint8(binary.LittleEndian.Uint32(kbytes))
	for {
		element_of_bit_set := make([]byte, 4)
		_, err := f.Read(element_of_bit_set)
		if err == io.EOF {
			break
		}
		int_element_of_bit_set := binary.LittleEndian.Uint32(element_of_bit_set)
		bit_set = append(bit_set, uint8(int_element_of_bit_set))
	}

	hll.reg = bit_set

	return hll
}