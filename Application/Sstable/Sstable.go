package Sstable

import (
	"awesomeProject5/Application/BloomFilter"
	"awesomeProject5/Application/MerkleTree"
	"awesomeProject5/Application/WriteAheadLog"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

type Location struct {
	key       string
	value     int
	keylenght int
}

type Sstable struct {
	Summary     []Location
	Index       []Location
	Data        []WriteAheadLog.Line
	BloomFilter BloomFilter.BloomFilter
}

func serializeindex(loc Location) []byte {
	var allbytes []byte
	keylenbytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(keylenbytes, uint32(len(loc.key)))
	keybytes := []byte(loc.key)
	locationbytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(locationbytes, uint64(loc.value))
	allbytes = append(allbytes, keylenbytes...)
	allbytes = append(allbytes, keybytes...)
	allbytes = append(allbytes, locationbytes...)
	return allbytes
}

func serializeBloom(filter BloomFilter.BloomFilter) []byte {
	var allbytes []byte
	mbytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(mbytes, uint64(filter.M))
	kbytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(kbytes, uint64(filter.K))
	allbytes = append(allbytes, mbytes...)
	allbytes = append(allbytes, kbytes...)
	set := filter.BitSet
	for line := range set {
		elem := make([]byte, 4)
		binary.LittleEndian.PutUint32(elem, uint32(set[line]))
		allbytes = append(allbytes, elem...)
	}
	return allbytes
}

func (sst *Sstable) Init(data []WriteAheadLog.Line) {
	sst.Data = data
	sst.BloomFilter = BloomFilter.BloomFilter{}
	sst.BloomFilter.M = BloomFilter.CalculateM(len(data), 0.005)
	sst.BloomFilter.K = BloomFilter.CalculateK(len(data), sst.BloomFilter.M)
	sst.BloomFilter.HashFunctions = BloomFilter.CreateHashFunctions(sst.BloomFilter.K)
	sst.BloomFilter.CreateBitSet()
	for line := range data {
		sst.BloomFilter.AddElement(data[line].Key)
	}
	f, err := os.Open("Data")
	if err != nil {
		_, err := os.Create("Data")
		if err != nil {
			return
		}
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {

		}
	}(f)

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		sst.WriteData(1)
		return
	}
	files, err := ioutil.ReadDir("Data/")
	if err != nil {
		log.Fatal(err)
	}
	var intval int
	for _, file := range files {
		if strings.Contains(file.Name(), "Sstable") {
			sliced := strings.Split(file.Name(), ".")
			sliced2 := strings.Split(sliced[0], "Sstable-")
			intval, _ = strconv.Atoi(sliced2[1])
		}
	}
	sst.WriteData(intval + 1)
}

func (sst *Sstable) WriteData(segment int) {
	pad := fmt.Sprintf("%04d", segment)
	filename := "Data/Sstable-" + pad + ".bin"
	filename1 := "Data/Index-" + pad + ".bin"
	filename2 := "Data/Summary-" + pad + ".bin"
	filename3 := "Data/BloomFilter-" + pad + ".bin"
	filename4 := "Data/Metadata-" + pad + ".txt"
	_, _ = os.Create(filename)
	_, _ = os.Create(filename1)
	_, _ = os.Create(filename2)
	_, _ = os.Create(filename3)
	_, _ = os.Create(filename4)
	var bloombytes []byte
	var sstablebytes []byte
	var indexbytes []byte
	var summarybytes []byte
	var loc Location
	var summaryloc Location
	bloombytes = serializeBloom(sst.BloomFilter)
	for line := range sst.Data {
		dline := sst.Data[line]
		currentks := dline.Keysize
		currentvs := dline.Valuesize
		sstablebytes = append(sstablebytes, WriteAheadLog.SerializeLine(dline)...)
		loc = Location{dline.Key, len(sstablebytes) - int(currentks) - int(currentvs), len(dline.Key)}
		sst.Index = append(sst.Index, loc)
		indexbytes = append(indexbytes, serializeindex(loc)...)
		if line == 0 || line == len(sst.Data)-1 {
			summaryloc = Location{dline.Key, len(summarybytes) - int(currentks) - loc.value, len(dline.Key)}
			sst.Summary = append(sst.Summary, summaryloc)
			summarybytes = append(summarybytes, serializeindex(summaryloc)...)
		}
	}
	file, _ := os.OpenFile(filename, os.O_APPEND, 0777)
	file1, _ := os.OpenFile(filename1, os.O_APPEND, 0777)
	file2, _ := os.OpenFile(filename2, os.O_APPEND, 0777)
	file3, _ := os.OpenFile(filename3, os.O_APPEND, 0777)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)
	defer func(file1 *os.File) {
		err := file1.Close()
		if err != nil {

		}
	}(file1)
	defer func(file2 *os.File) {
		err := file2.Close()
		if err != nil {

		}
	}(file2)
	defer func(file3 *os.File) {
		err := file3.Close()
		if err != nil {

		}
	}(file3)
	_, err := file.Write(sstablebytes)
	if err != nil {
		return
	}
	_, err = file1.Write(indexbytes)
	if err != nil {
		return
	}
	_, err = file2.Write(summarybytes)
	if err != nil {
		return
	}
	_, err = file3.Write(bloombytes)
	if err != nil {
		return
	}
	mt := MerkleTree.MerkleTree{}
	mt.BuildMT(MerkleTree.CreateNodes(sst.Data))
	mt.WriteTree(filename4)

}
