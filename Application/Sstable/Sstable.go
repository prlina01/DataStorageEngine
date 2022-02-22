package Sstable

import (
	"KeyDataStorage/Application/BloomFilter"
	"KeyDataStorage/Application/HyperLogLog"
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
	"sort"
	"strconv"
	"strings"
)

type LsmTreeConfig struct {
	FalsePRate   float64	`yaml:"false_positive_rate"`
	HLLPrecision uint8	`yaml:"hll_precision"`
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
	Summary      Summary
	Index        []Location
	Data         []WriteAheadLog.Line
	BloomFilter  BloomFilter.BloomFilter
	HLL		     HyperLogLog.HLL
	FalsePRate   float64
	HLLPrecision uint8
	Identifier   string
}

func FindKey(searchKey string) []byte {
	sstables := GetKeyDataStructure()
	keys := make([]int, 0)
	for k := range sstables {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	for _, k := range keys {

		for _, sstable := range sstables[uint32(k)] {

			fBloomfilter, err := os.Open("Data/" + sstable["BloomFilter"])
			bloomFilter := ParseBloom(fBloomfilter)
			if !bloomFilter.IsElementInBloomFilter(searchKey) {
				err = fBloomfilter.Close()
				continue
			}
			err = fBloomfilter.Close()
			if err != nil {
				return nil
			}
			if err != nil {
				return nil
			}
			fSummary, err := os.Open("Data/" + sstable["Summary"])
			if err != nil {
				return nil
			}
			if err!= nil {panic("Can't open file!")}
			indexOffsetNeeded := 0
			summary := ParseSummary(fSummary)
			err = fSummary.Close()
			if err != nil {
				return nil
			}
			if searchKey >= summary.first.key && searchKey <= summary.last.key {
				for _, summaryLine := range summary.index {
					if summaryLine.key == searchKey {
						indexOffsetNeeded = summaryLine.value
					}
				}
				if indexOffsetNeeded == 0 { continue }
			} else { continue }

			fIndex, err := os.Open("Data/" + sstable["Index"])
			if err!= nil {panic("Can't open file!")}
			_, _ = fIndex.Seek(int64(indexOffsetNeeded), 0)
			//ParseIndex(f_index, searchKey)
			indexLine := ParseIndexLine(fIndex)
			err = fIndex.Close()
			if err != nil {
				return nil
			}
			dataOffsetNeeded := indexLine.value



			fData, err := os.Open("Data/" + sstable["Sstable"])
			if err!= nil {panic("Can't open file!")}
			_, _ = fData.Seek(int64(dataOffsetNeeded), 0)
			dataLine, err := WriteAheadLog.ParseLine(fData)
			err = fData.Close()
			if err != nil {
				return nil
			}
			if err!= nil {panic("Can't read from file!")}
			if dataLine.Key == searchKey {
				return dataLine.Value
			}
		}
	}
	return nil
}

func GetKeyDataStructure() map[uint32][]map[string]string {
	fileNames := Utils.Find(".*Sstable.*")
	sort.Slice(fileNames, func(i, j int) bool {
		firstSplit := strings.Split(fileNames[i], ".")[0]
		secondSplit := strings.Split(firstSplit, "-")

		firstSplit2 := strings.Split(fileNames[j], ".")[0]
		secondSplit2 := strings.Split(firstSplit2, "-")


		identifierMul, _ := strconv.Atoi(secondSplit[2])
		identifierMul2, _ := strconv.Atoi(secondSplit2[2])

		return identifierMul > identifierMul2
	})
	//var sstables map[uint32][]map[string]string
	sstables := make(map[uint32][]map[string]string)
	for _, fileName := range fileNames {
		firstSplit := strings.Split(fileName, ".")[0]
		secondSplit := strings.Split(firstSplit, "-")

		identifierMul, err := strconv.Atoi(secondSplit[0])
		if err!= nil {panic("Can't convert value!")}

		connectedFilesMap := make(map[string]string)

		connectedFiles := Utils.Find(".*"+secondSplit[2])
		for _, connectedFile := range connectedFiles {
			connectedFilesMap["Sstable"] = fileName
			if strings.Contains(connectedFile, "BloomFilter") {
				connectedFilesMap["BloomFilter"] = connectedFile
			} else 	if strings.Contains(connectedFile, "Index") {
				connectedFilesMap["Index"] = connectedFile
			} else 	if strings.Contains(connectedFile, "Summary") {
				connectedFilesMap["Summary"] = connectedFile
			}else if strings.Contains(connectedFile, "HyperLogLog") {
				connectedFilesMap["HyperLogLog"] = connectedFile
			}
		}
		sstables[uint32(identifierMul)] = append(sstables[uint32(identifierMul)], connectedFilesMap)
	}
	return sstables
}

func  DeleteSSTableAndConnectedParts(path string) {
	identifierMul := strings.Split(path, ".")
	identifierMul = strings.Split(identifierMul[0], "-")
	identifier := identifierMul[2]
	files, err := ioutil.ReadDir("Data/")
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		sliced := strings.Split(file.Name(), ".")
		identifierOther := strings.Split(sliced[0], "-")[2]
		if identifier == identifierOther {
			e:= os.Remove("Data/" + file.Name())
			if e!= nil {
				fmt.Println(e)
				panic("Can't delete file")
			}
		}

	}

}

func Compaction(FalsePRate   float64, HLLPrecision uint8,  MaxLsmTreeLevel int, MaxLsmNodesFirstLevel int, MaxLsmNodesOtherLevels int){

	level := 1
	for level < MaxLsmTreeLevel {
		fileNames := Utils.Find(strconv.Itoa(level)+".*Sstable.*")

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

			if len(fileNames) < MaxLsmNodesFirstLevel {
				fmt.Println("Not enough SStables for compaction, try again later.")
				return
			}

		}

		// izlazak
		if level > 1 && len(fileNames) < MaxLsmNodesOtherLevels {return}

		var lines []WriteAheadLog.Line

		f1, _ := os.Open("Data/" + fileNames[0])
		f2, _ := os.Open("Data/" + fileNames[1])
		sst := Sstable{FalsePRate: FalsePRate, HLLPrecision:HLLPrecision}
		if level <= 1 {
			for i:=1; i < MaxLsmNodesFirstLevel; i++ {
				if i >= 2 {
					f1, _ = os.Open(sst.Identifier)
					f2, _ = os.Open("Data/" + fileNames[i])
				}

				for {
					line1, err1 := WriteAheadLog.ParseLine(f1)
					line2, err2 := WriteAheadLog.ParseLine(f2)

					if err1==nil && err2!=nil {
						lines = append(lines, line1)
						continue
					} else if err1!=nil && err2==nil {
						lines = append(lines, line2)
						continue
					} else if err1!=nil && err2!=nil {break}

					if line1.Key < line2.Key {
						lines = append(lines, line1)
						_, _ = f2.Seek(-29-int64(line2.Keysize)-int64(line2.Valuesize), 1)
					} else if line2.Key < line1.Key {
						lines = append(lines, line2)
						_, err := f1.Seek(-29-int64(line1.Keysize)-int64(line1.Valuesize), 1)
						if err != nil {
							return 
						}
					} else if line1.Key == line2.Key {
						if line1.Tombstone == 1 && line2.Tombstone == 1 {
							fmt.Println(line1.Key + " has been deleted!")
							continue
						} else if line1.Tombstone == 1 && line1.Timestamp < line2.Timestamp {
							lines = append(lines, line2)
							continue
						} else if line1.Tombstone == 1 && line1.Timestamp > line2.Timestamp  {
							fmt.Println(line1.Key + " has been deleted!")
							continue
						} else if line2.Tombstone == 1 && line2.Timestamp < line1.Timestamp {
							lines = append(lines, line1)
							continue
						} else if line2.Tombstone == 1 && line2.Timestamp > line1.Timestamp {
							fmt.Println(line2.Key + "has been deleted")
							continue
						} else if line1.Timestamp > line2.Timestamp {
							lines = append(lines, line1)
						} else {lines = append(lines, line2)}
					}
				}

				err := f1.Close()
				if err != nil {
					panic("Error closing file")
				}
				err2 := f2.Close()
				if err2 != nil {
					panic("Error closing file")
				}

				DeleteSSTableAndConnectedParts(f1.Name())
				DeleteSSTableAndConnectedParts(f2.Name())

				sst = Sstable{FalsePRate: FalsePRate, HLLPrecision:HLLPrecision}
				sst.Init(lines, level)
				lines = []WriteAheadLog.Line{}

			}
		} else {
			for i:=1; i < MaxLsmNodesOtherLevels; i++ {
				if i >= 2 {
					f1, _ = os.Open(sst.Identifier)
					f2, _ = os.Open(fileNames[i])
				}

				for {
					line1, err1 := WriteAheadLog.ParseLine(f1)
					line2, err2 := WriteAheadLog.ParseLine(f2)

					if err1==nil && err2!=nil {
						lines = append(lines, line1)
						continue
					} else if err1!=nil && err2==nil {
						lines = append(lines, line2)
						continue
					} else if err1!=nil && err2!=nil {break}

					if line1.Key < line2.Key {
						lines = append(lines, line1)
						_, _ = f2.Seek(-29-int64(line2.Keysize)-int64(line2.Valuesize), 1)
					} else if line2.Key < line1.Key {
						lines = append(lines, line2)
						_, _ = f1.Seek(-29-int64(line1.Keysize)-int64(line1.Valuesize), 1)
					} else if line1.Key == line2.Key {
						if line1.Tombstone == 1 && line2.Tombstone == 1 {
							fmt.Println(line1.Key + " has been deleted!")
							continue
						} else if line1.Tombstone == 1 && line1.Timestamp < line2.Timestamp {
							lines = append(lines, line2)
							continue
						} else if line1.Tombstone == 1 && line1.Timestamp > line2.Timestamp  {
							fmt.Println(line1.Key + " has been deleted!")
							continue
						} else if line2.Tombstone == 1 && line2.Timestamp < line1.Timestamp {
							lines = append(lines, line1)
							continue
						} else if line2.Tombstone == 1 && line2.Timestamp > line1.Timestamp {
							fmt.Println(line2.Key + "has been deleted")
							continue
						} else if line1.Timestamp > line2.Timestamp {
							lines = append(lines, line1)
						} else {lines = append(lines, line2)}
					}
				}

				err := f1.Close()
				if err != nil {
					panic("Error closing file")
				}
				err2 := f2.Close()
				if err2 != nil {
					panic("Error closing file")
				}

				DeleteSSTableAndConnectedParts(f1.Name())
				DeleteSSTableAndConnectedParts(f2.Name())

				sst = Sstable{FalsePRate: FalsePRate, HLLPrecision:HLLPrecision}
				sst.Init(lines, level)
				lines = []WriteAheadLog.Line{}

			}
		}


		newLevelSst := Sstable{FalsePRate: FalsePRate, HLLPrecision:HLLPrecision}

		DeleteSSTableAndConnectedParts(sst.Identifier)
		newLevelSst.Init(sst.Data, level + 1)


		level += 1
	}

}



func serializeindex(loc Location) []byte {
	//summaryloc = Location{dline.Key, len(indexbytes) - int(currentks) - 12, len(dline.Key)}
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

func ParseIndex(f *os.File, key string) []Location {
	var locations []Location

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
		if location.key == key {
			currentPos, _ := f.Seek(0, 1)
			fmt.Println(currentPos)
		}
		locationbytes := make([]byte, 8)
		_, _ = f.Read(locationbytes)
		location.value = int(binary.LittleEndian.Uint64(locationbytes))
		locations = append(locations, location)
	}


	return locations
}

func ParseIndexLine(f *os.File) Location {
	//allbytes = append(allbytes, keylenbytes...)
	//allbytes = append(allbytes, keybytes...)
	//allbytes = append(allbytes, locationbytes...)
	location := Location{}
	keylenbytes := make([]byte, 4)
	_, err := f.Read(keylenbytes)
	if err == io.EOF {
		return location
	}
	location.keylenght = int(binary.LittleEndian.Uint32(keylenbytes))

	keybytes := make([]byte, location.keylenght)
	_, _ = f.Read(keybytes)
	location.key = string(keybytes)

	locationbytes := make([]byte, 8)
	_, _ = f.Read(locationbytes)
	location.value = int(binary.LittleEndian.Uint64(locationbytes))
	return location

}

func ParseSummary(f *os.File) Summary {
	summary := Summary{}
	var indexes []Location
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
	var bitSet []int
	mbytes := make([]byte, 8)
	_, _ = f.Read(mbytes)
	bloom.M = uint(binary.LittleEndian.Uint64(mbytes))
	kbytes := make([]byte, 8)
	_, _ = f.Read(kbytes)
	bloom.K = uint(binary.LittleEndian.Uint64(kbytes))

	var h []hash.Hash32
	//ts := uint(time.Now().Unix())

	hashConfigBytes := make([]byte, 4)
	_, _ = f.Read(hashConfigBytes)
	bloom.HashFunctionsConfig = binary.LittleEndian.Uint32(hashConfigBytes)

	for i := uint(0); i < bloom.K; i++ {
		h = append(h, murmur3.New32WithSeed(bloom.HashFunctionsConfig+uint32(i)))
	}
	bloom.HashFunctions = h

	for {
		elementOfBitSet := make([]byte, 4)
		_, err := f.Read(elementOfBitSet)
		if err == io.EOF {
			break
		}
		intElementOfBitSet := binary.LittleEndian.Uint32(elementOfBitSet)
		bitSet = append(bitSet, int(intElementOfBitSet))
	}

	bloom.BitSet = bitSet

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
	elemHash := make([]byte, 4)
	binary.LittleEndian.PutUint32(elemHash, hashConfiguration)
	allbytes = append(allbytes, elemHash...)

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

func (sst *Sstable) Init(data []WriteAheadLog.Line, lsmLevel int) {
	sst.Data = data
	sst.BloomFilter = BloomFilter.BloomFilter{}
	sst.BloomFilter.M = BloomFilter.CalculateM(len(data), sst.FalsePRate)
	sst.BloomFilter.K = BloomFilter.CalculateK(len(data), sst.BloomFilter.M)
	sst.BloomFilter.HashFunctions, sst.BloomFilter.HashFunctionsConfig = BloomFilter.CreateHashFunctions(sst.BloomFilter.K)
	sst.BloomFilter.CreateBitSet()
	sst.HLL.Create_array(sst.HLLPrecision)
	for line := range data {
		sst.BloomFilter.AddElement(data[line].Key)
		sst.HLL.Add_element(data[line].Key)
	}
	f, err := os.Open("Data")
	if err != nil {
		_, err := os.Create("Data")
		if err != nil {
			return
		}
	}
	err = f.Close()
	if err != nil {
		return
	}

	//_, err = f.Readdirnames(1)
	//if err == io.EOF {
	//	sst.WriteData(1 ,1)
	//	return
	//}
	files, err := ioutil.ReadDir("Data/")
	if err != nil {
		log.Fatal(err)
	}
	var intval int
	var intvals []int
	for _, file := range files {
		if strings.Contains(file.Name(), "Sstable") {
			sliced := strings.Split(file.Name(), ".")
			sliced2 := strings.Split(sliced[0], "Sstable-")
			intval, _ = strconv.Atoi(sliced2[1])
			intvals = append(intvals, intval)

		}
	}
	max := 0
	if len(intvals) > 0 {
		max = intvals[0]
		for _, value := range intvals {
			if value > max {max = value}
		}
	}



	sst.WriteData(max + 1, lsmLevel)
}
func ParseData(f *os.File) []WriteAheadLog.Line {
	var whlLines []WriteAheadLog.Line
	parsedLine, _ := WriteAheadLog.ParseLine(f)
	whlLines = append(whlLines, parsedLine )

	return whlLines
}

func (sst *Sstable) WriteData(segment int, lsmLevel int) {
	pad := fmt.Sprintf("%04d", segment)


	filename := "Data/" + strconv.Itoa(lsmLevel) + "-Sstable-" + pad + ".db"
	sst.Identifier = filename
	filename1 := "Data/" + strconv.Itoa(lsmLevel) +  "-Index-" + pad + ".db"
	filename2 := "Data/"+ strconv.Itoa(lsmLevel) + "-Summary-" + pad + ".db"
	filename3 := "Data/"+ strconv.Itoa(lsmLevel) + "-BloomFilter-" + pad + ".db"
	filename4 := "Data/"+ strconv.Itoa(lsmLevel) + "-Metadata-" + pad + ".txt"
	filename5 := "Data/"+ strconv.Itoa(lsmLevel) + "-TOC-" + pad + ".db"
	filename6 := "Data/"+ strconv.Itoa(lsmLevel) + "-HyperLogLog-" + pad + ".db"

	f1, _ := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0777)
	f2, _ := os.OpenFile(filename1, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0777)
	f3, _ := os.OpenFile(filename2, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0777)
	f4, _ := os.OpenFile(filename3, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0777)
	f5, _ := os.OpenFile(filename4, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0777)
	f6, _ := os.OpenFile(filename5, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0777)
	f7, _ := os.OpenFile(filename6, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0777)
	err := f1.Close()
	if err != nil {
		return 
	}
	err = f2.Close()
	if err != nil {
		return 
	}
	err = f3.Close()
	if err != nil {
		return 
	}
	err = f4.Close()
	if err != nil {
		return 
	}
	err = f5.Close()
	if err != nil {
		return 
	}
	err = f6.Close()
	if err != nil {
		return 
	}
	err = f7.Close()
	if err != nil {
		return
	}
	var stringss []string
	stringss = append(stringss,strings.Split(filename,"Data/")[1]+"\n")
	stringss = append(stringss,strings.Split(filename1,"Data/")[1]+"\n")
	stringss = append(stringss,strings.Split(filename2,"Data/")[1]+"\n")
	stringss = append(stringss,strings.Split(filename3,"Data/")[1]+"\n")
	stringss = append(stringss,strings.Split(filename4,"Data/")[1]+"\n")
	stringss = append(stringss,strings.Split(filename5,"Data/")[1]+"\n")
	stringss = append(stringss,strings.Split(filename6,"Data/")[1])
	var bloombytes []byte
	var hllbytes []byte
 	var sstablebytes []byte
	var indexbytes []byte
	var summarybytes []byte
	var firstlast []byte
 	var loc Location
	var summaryloc Location
	var first Location
	var last Location
	bloombytes = serializeBloom(sst.BloomFilter)
	hllbytes = sst.HLL.Serialize()
	for line := range sst.Data {
		dline := sst.Data[line]
		currentks := dline.Keysize
		currentvs := dline.Valuesize
		sstablebytes = append(sstablebytes, WriteAheadLog.SerializeLine(dline)...)
		loc = Location{dline.Key, len(sstablebytes) - int(currentks) - int(currentvs) - 29, len(dline.Key)}
		sst.Index = append(sst.Index, loc)
		indexbytes = append(indexbytes, serializeindex(loc)...)
		summaryloc = Location{dline.Key, len(indexbytes) - int(currentks) - 12, len(dline.Key)}
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
	file, _ := os.OpenFile(filename, os.O_APPEND | os.O_WRONLY, 0777)
	file1, _ := os.OpenFile(filename1, os.O_APPEND | os.O_WRONLY, 0777)
	file2, _ := os.OpenFile(filename2, os.O_APPEND | os.O_WRONLY, 0777)
	file3, _ := os.OpenFile(filename3, os.O_APPEND | os.O_WRONLY, 0777)
	file5, _ := os.OpenFile(filename5, os.O_APPEND | os.O_WRONLY, 0777)
	file6, _ := os.OpenFile(filename6, os.O_APPEND | os.O_WRONLY, 0777)
	_, err = file.Write(sstablebytes)
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
	_, err = file6.Write(hllbytes)
	if err != nil {
		return
	}
	mt := MerkleTree.MerkleTree{}
	mt.BuildMT(MerkleTree.CreateNodes(sst.Data))
	mt.WriteTree(filename4)
	for line:=range stringss {
		_, err := file5.WriteString(stringss[line])
		if err != nil {
			return 
		}
	}
	err = file.Close()
	if err != nil {
		return 
	}
	err = file1.Close()
	if err != nil {
		return 
	}
	err = file2.Close()
	if err != nil {
		return 
	}
	err = file3.Close()
	if err != nil {
		return 
	}
	err = file5.Close()
	if err != nil {
		return 
	}
	err = file6.Close()
	if err != nil {
		return
	}



}
