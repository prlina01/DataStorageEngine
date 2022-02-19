package Sstable

import (
	"KeyDataStorage/Application/BloomFilter"
	"KeyDataStorage/Application/MerkleTree"
	"KeyDataStorage/Application/Utils"
	"KeyDataStorage/Application/WriteAheadLog"
	"encoding/binary"
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

)

type LsmTreeConfig struct {
	MaxLsmTreeLevel int `yaml:"max_lsm_tree_level"`
	MaxLsmNodesFirstLevel int `yaml:"max_lsm_nodes_first_level"`
	MaxLsmNodesOtherLevels int `yaml:"max_lsm_nodes_other_levels"`
}

type Summary struct{
	first Location
	last Location
	index []Location
}

type Location struct {
	key       string
	value     int
	keylenght int
}

type Sstable struct {
	Summary     Summary
	Index       []Location
	Data        []WriteAheadLog.Line
	BloomFilter BloomFilter.BloomFilter
	Identifier string
}



func Compaction() {
	var config LsmTreeConfig
	level := 1
	for level < config.MaxLsmTreeLevel {
		fileNames := Utils.Find(strconv.Itoa(level)+"Sstable.*")

		if len(fileNames) == 0 {
			if level == 1 {
				fmt.Println("No Sstables at the moment")
			}
			return
		}

		if level == 1 {
			if len(fileNames) == 1{
				fmt.Println("Can not do compaction on only one Sstable!")
				return
			}

			if len(fileNames) < config.MaxLsmNodesFirstLevel {
				fmt.Println("Not enough SStables for compaction, try again later.")
			}

			lines := []WriteAheadLog.Line{}

			f1, _ := os.Open(fileNames[0])
			f2, _ := os.Open(fileNames[1])
			sst := Sstable{}
			for i:=1; i < config.MaxLsmNodesFirstLevel; i++ {
				if i >= 2 {
					f1, _ = os.Open(sst.Identifier)
					f2, _ = os.Open(fileNames[i])
				}

				for {
					line1, err1 := WriteAheadLog.ParseLine(f1)
					line2, err2 := WriteAheadLog.ParseLine(f2)

					if err1==nil && err2!=nil {
						lines = append(lines, line1)
					} else if err1!=nil && err2==nil {
						lines = append(lines, line2)
					} else if err1!=nil && err2!=nil {break}

					if line1.Key < line2.Key {
						lines = append(lines, line1)
						f2.Seek(int64(-29-line2.Keysize-line2.Valuesize), 1)
					} else if line2.Key < line1.Key {
						lines = append(lines, line2)
						f1.Seek(int64(-29-line1.Keysize-line1.Valuesize), 1)
					} else if line1.Key == line2.Key {
						if line1.Tombstone == 1 || line2.Tombstone == 1 {
							fmt.Println(line1.Key + " obrisan!")
							continue
						} else if line1.Timestamp > line2.Timestamp {
							lines = append(lines, line1)
						} else {lines = append(lines, line2)}
					}
				}



				sst = Sstable{}
				sst.Init(lines)
				lines = []WriteAheadLog.Line{}



			}
		}





		// izlazak
		if level > 1 && len(fileNames) < config.MaxLsmNodesOtherLevels {return}


		}


		level += 1
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

func ParseIndex(f *os.File) []Location {
	locations := []Location{}

	for {
		location := Location{}
		keylenbytes := make([]byte, 4)
		_, err := f.Read(keylenbytes)
		if err == io.EOF {
			break
		}
		location.keylenght = int(binary.LittleEndian.Uint32(keylenbytes))

		keybytes := make([]byte, location.keylenght)
		_, _ = f.Read(keybytes)
		location.key = string(keybytes)

		locationbytes := make([]byte, 8)
		_, _ = f.Read(locationbytes)
		location.value = int(binary.LittleEndian.Uint64(locationbytes))
		locations = append(locations, location)
	}


	return locations
}

func ParseSummary(f *os.File) Summary {
	summary := Summary{}
	indexes := []Location{}
	firstline := Location{}
	lastline := Location{}
	keylenbytes := make([]byte, 4)
	_, _ = f.Read(keylenbytes)
	firstline.keylenght = int(binary.LittleEndian.Uint32(keylenbytes))

	keybytes := make([]byte, firstline.keylenght)
	_, _ = f.Read(keybytes)
	firstline.key = string(keybytes)

	locationbytes := make([]byte, 8)
	_, _ = f.Read(locationbytes)
	firstline.value = int(binary.LittleEndian.Uint64(locationbytes))

	keylenbytes2 := make([]byte, 4)
	_, _ = f.Read(keylenbytes2)
	lastline.keylenght = int(binary.LittleEndian.Uint32(keylenbytes2))

	keybytes2 := make([]byte, lastline.keylenght)
	_, _ = f.Read(keybytes2)
	lastline.key = string(keybytes2)

	locationbytes2 := make([]byte, 8)
	_, _ = f.Read(locationbytes)
	lastline.value = int(binary.LittleEndian.Uint64(locationbytes2))

	summary.first = firstline
	summary.last = lastline

	for {
		index := Location{}
		keylenbytes := make([]byte, 4)
		_, err := f.Read(keylenbytes)
		if err == io.EOF {
			break
		}
		index.keylenght = int(binary.LittleEndian.Uint32(keylenbytes))

		keybytes := make([]byte, index.keylenght)
		_, _ = f.Read(keybytes)
		index.key = string(keybytes)

		locationbytes := make([]byte, 8)
		_, _ = f.Read(locationbytes)
		index.value = int(binary.LittleEndian.Uint64(locationbytes))
		indexes = append(indexes, index)
	}

	summary.index = indexes

	return summary
}


func ParseBloom(f *os.File) BloomFilter.BloomFilter {
	bloom := BloomFilter.BloomFilter{}
	var bit_set []int
	mbytes := make([]byte, 8)
	_, _ = f.Read(mbytes)
	bloom.M = uint(binary.LittleEndian.Uint64(mbytes))
	kbytes := make([]byte, 8)
	_, _ = f.Read(kbytes)
	bloom.K = uint(binary.LittleEndian.Uint64(kbytes))

	h := []hash.Hash32{}
	//ts := uint(time.Now().Unix())

	hashConfigBytes := make([]byte, 4)
	_, _ = f.Read(hashConfigBytes)
	bloom.HashFunctionsConfig = uint32(binary.LittleEndian.Uint32(hashConfigBytes))

	for i := uint(0); i < bloom.K; i++ {
		h = append(h, murmur3.New32WithSeed(bloom.HashFunctionsConfig+uint32(i)))
	}
	bloom.HashFunctions = h

	for {
		element_of_bit_set := make([]byte, 4)
		_, err := f.Read(element_of_bit_set)
		if err == io.EOF {
			break
		}
		int_element_of_bit_set := binary.LittleEndian.Uint32(element_of_bit_set)
		bit_set = append(bit_set, int(int_element_of_bit_set))
	}

	bloom.BitSet = bit_set

	return bloom
}

func serializeBloom(filter BloomFilter.BloomFilter) []byte {
	var allbytes []byte
	mbytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(mbytes, uint64(filter.M))
	kbytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(kbytes, uint64(filter.K))
	allbytes = append(allbytes, mbytes...)
	allbytes = append(allbytes, kbytes...)

	hashConfiguration := filter.HashFunctionsConfig
	elem_hash := make([]byte, 4)
	binary.LittleEndian.PutUint32(elem_hash, uint32(hashConfiguration))
	allbytes = append(allbytes, elem_hash...)

	set := filter.BitSet
	for line := range set {
		elem := make([]byte, 4)
		binary.LittleEndian.PutUint32(elem, uint32(set[line]))
		allbytes = append(allbytes, elem...)
	}
	return allbytes
}

func (sst *Sstable) findPath() {

}

func (sst *Sstable) Init(data []WriteAheadLog.Line) {
	sst.Data = data
	sst.BloomFilter = BloomFilter.BloomFilter{}
	sst.BloomFilter.M = BloomFilter.CalculateM(len(data), 0.005)
	sst.BloomFilter.K = BloomFilter.CalculateK(len(data), sst.BloomFilter.M)
	sst.BloomFilter.HashFunctions, sst.BloomFilter.HashFunctionsConfig = BloomFilter.CreateHashFunctions(sst.BloomFilter.K)
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
		sst.WriteData(1 ,1)
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
	sst.WriteData(intval + 1, 1)
}
func ParseData(f *os.File) []WriteAheadLog.Line {
	whlLines := []WriteAheadLog.Line{}
	parsedLine, _ := WriteAheadLog.ParseLine(f)
	whlLines = append(whlLines, parsedLine )

	return whlLines
}

func (sst *Sstable) WriteData(segment int, lsmLevel int) {
	pad := fmt.Sprintf("%04d", segment)


	filename := "Data/" + string(rune(lsmLevel)) + "-Sstable-" + pad + ".db"
	sst.Identifier = filename
	filename1 := "Data/" + string(rune(lsmLevel)) + "-Index-" + pad + ".db"
	filename2 := "Data/" + string(rune(lsmLevel)) + "-Summary-" + pad + ".db"
	filename3 := "Data/" + string(rune(lsmLevel)) + "-BloomFilter-" + pad + ".db"
	filename4 := "Data/" + string(rune(lsmLevel)) + "-Metadata-" + pad + ".txt"
	filename5 := "Data/TOC-" + pad + ".db"
	_, _ = os.Create(filename)
	_, _ = os.Create(filename1)
	_, _ = os.Create(filename2)
	_, _ = os.Create(filename3)
	_, _ = os.Create(filename4)
	_, _ = os.Create(filename5)
	var stringss []string
	stringss = append(stringss,strings.Split(filename,"Data/")[1]+"\n")
	stringss = append(stringss,strings.Split(filename1,"Data/")[1]+"\n")
	stringss = append(stringss,strings.Split(filename2,"Data/")[1]+"\n")
	stringss = append(stringss,strings.Split(filename3,"Data/")[1]+"\n")
	stringss = append(stringss,strings.Split(filename4,"Data/")[1]+"\n")
	stringss = append(stringss,strings.Split(filename5,"Data/")[1])
	var bloombytes []byte
	var sstablebytes []byte
	var indexbytes []byte
	var summarybytes []byte
	var firstlast []byte
 	var loc Location
	var summaryloc Location
	var first Location
	var last Location
	bloombytes = serializeBloom(sst.BloomFilter)
	for line := range sst.Data {
		dline := sst.Data[line]
		currentks := dline.Keysize
		currentvs := dline.Valuesize
		sstablebytes = append(sstablebytes, WriteAheadLog.SerializeLine(dline)...)
		loc = Location{dline.Key, len(sstablebytes) - int(currentks) - int(currentvs), len(dline.Key)}
		sst.Index = append(sst.Index, loc)
		indexbytes = append(indexbytes, serializeindex(loc)...)
		summaryloc = Location{dline.Key, len(indexbytes) - int(currentks) - 8, len(dline.Key)}
		if line == 0{
			first = summaryloc
		}
		if line == len(sst.Data)-1{
			last = summaryloc
		}
		sst.Summary.index = append(sst.Summary.index, summaryloc)
		summarybytes = append(summarybytes, serializeindex(summaryloc)...)
	}
	sst.Summary.first = first
	sst.Summary.last = last
	firstlast = append(firstlast,serializeindex(first)...)
	firstlast = append(firstlast,serializeindex(last)...)
	firstlast = append(firstlast,summarybytes...)
	file, _ := os.OpenFile(filename, os.O_APPEND, 0777)
	file1, _ := os.OpenFile(filename1, os.O_APPEND, 0777)
	file2, _ := os.OpenFile(filename2, os.O_APPEND, 0777)
	file3, _ := os.OpenFile(filename3, os.O_APPEND, 0777)
	file5, _ := os.OpenFile(filename5, os.O_APPEND, 0777)
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
	_, err = file2.Write(firstlast)
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
	for line:=range(stringss){
		_, err := file5.WriteString(stringss[line])
		if err != nil {
			return 
		}
	}


}
