package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (16B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

const (
	T_SIZE = 8
	C_SIZE = 4

	CRC_SIZE       = T_SIZE + C_SIZE
	TOMBSTONE_SIZE = CRC_SIZE + 1
	KEY_SIZE       = TOMBSTONE_SIZE + T_SIZE
	VALUE_SIZE     = KEY_SIZE + T_SIZE
)

type Line struct{
	crc uint32
	timestamp uint64
	tombstone byte
	keysize uint64
	valuesize uint64
	key string
	value []byte
}

type WriteAheadLog struct{
	file string
	data    []Line
	segLoc  []int
	segment int64
}

func (wal *WriteAheadLog) init(segment int64){
	wal.segment = segment;
	f,err := os.Open("wal")
	if err != nil{
		os.Create("wal")
	}
	defer f.Close()

	_,err = f.Readdirnames(1)
	if err == io.EOF{
		file,err := os.Create("wal/wal-0001.log")
		if err!=nil {
			panic("File already exists")
		}
		wal.file = "wal/wal-0001.log"
		wal.segLoc = append(wal.segLoc,1)
		file.Close()
		return
	}
	wal.read()
}

func (wal *WriteAheadLog) read(){
	files, err := ioutil.ReadDir("wal/")
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		sliced := strings.Split(file.Name(),".")
		sliced2 := strings.Split(sliced[0],"wal-")
		intval,_ := strconv.Atoi(sliced2[1])
		wal.segLoc = append(wal.segLoc,intval)
	}
	lastindex := wal.segLoc[len(wal.segLoc)-1]
	pad := fmt.Sprintf("%04d",lastindex)
	filename := "wal/wal-"+pad+".log"
	wal.file = filename
	wal.readSegment(lastindex)

}

func (wal *WriteAheadLog) readSegment(segment int) []Line {
	pad := fmt.Sprintf("%04d",segment)
	filename := "wal/wal-"+pad+".log"
	f, err := os.Open(filename)
	if err!=nil {
		panic("err")
	}
	var data []Line;
	crc := make([]byte,4)
	timestamp := make([]byte,8)
	tombstone := make([]byte,1)
	keysize := make([]byte,8)
	valuesize := make([]byte,8)
	for i:=1;int64(i)<=wal.segment;i++{
		line := Line{}
		_, err = f.Read(crc)
		crcint := binary.LittleEndian.Uint32(crc)
		line.crc = crcint
		_, err = f.Read(timestamp)
		timestampint := binary.LittleEndian.Uint64(timestamp)
		line.timestamp = timestampint
		_, err = f.Read(tombstone)
		line.tombstone = tombstone[0]
		_, err = f.Read(keysize)
		keysizeint := binary.LittleEndian.Uint64(keysize)
		line.keysize = keysizeint
		_, err = f.Read(valuesize)
		valuesizeint := binary.LittleEndian.Uint64(valuesize)
		line.valuesize = valuesizeint
		key := make([]byte,keysizeint)
		value := make([]byte,valuesizeint)
		_, err = f.Read(key)
		_, err = f.Read(value)
		line.key = string(key)
		line.value = value
		data = append(data, line)
		if errors.Is(err,io.EOF){
			return data
		}

	}
	return data
}

func (wal *WriteAheadLog) addKV(key string,value []byte){
	if int64(len(wal.data)) > wal.segment{
		files,_ := ioutil.ReadDir("wal/")
		i := len(files)+1
		wal.segLoc = append(wal.segLoc,i)
		pad := fmt.Sprintf("%04d",i)
		filename := "wal/wal-"+pad+".log"
		file,err := os.Create(filename)
		if err!=nil {
			panic("File already exists")
		}
		wal.data = nil
		wal.file = filename;
		file.Close()
	}
	myfile,err := os.OpenFile(wal.file,os.O_APPEND,0777)
	if err!=nil{
		panic("err")
	}
	mybytes := []byte(key)
	for i:=0;i< len(value);i++{
	mybytes = append(mybytes,value[i])
	}
	var allbytes []byte;
	chcks := CRC32(mybytes)
	chcksbytes := make([]byte,4)
	binary.LittleEndian.PutUint32(chcksbytes,chcks)

	t:= time.Now().Unix()
	fulltimestamp := uint(t)
	fulltimestampbytes := make([]byte,8)
	binary.LittleEndian.PutUint64(fulltimestampbytes, uint64(fulltimestamp))

	tombstone := byte(1)

	keysize := uint64(len(key))
	keysizebytes:= make([]byte,8)
	binary.LittleEndian.PutUint64(keysizebytes,keysize)

	valuesize := uint64(len(value))
	valuesizebytes := make([]byte,8)
	binary.LittleEndian.PutUint64(valuesizebytes,valuesize)

	keybytes := []byte(key)

	allbytes = append(allbytes, chcksbytes...)
	allbytes = append(allbytes, fulltimestampbytes...)
	allbytes = append(allbytes, tombstone)
	allbytes = append(allbytes, keysizebytes...)
	allbytes = append(allbytes, valuesizebytes...)
	allbytes = append(allbytes, keybytes...)
	allbytes = append(allbytes, value...)
	line := Line{chcks, uint64(fulltimestamp),tombstone,keysize,valuesize,key,value}
	wal.data = append(wal.data, line)
	myfile.Write(allbytes)


}


func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func appendWal(file *os.File, data []byte) error {
	currentLen, err := fileLen(file)
	if err != nil {
		return err
	}
	err = file.Truncate(currentLen + int64(len(data)))
	if err != nil {
		return err
	}
	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	copy(mmapf[currentLen:], data)
	mmapf.Flush()
	mmapf.Unmap()
	return nil
}
func fileLen(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func main() {
	wal := WriteAheadLog{}
	wal.init(5)
	wal.addKV("dog",make([]byte,5))
	wal.addKV("dog",make([]byte,5))
	wal.addKV("dog",make([]byte,5))
	wal.addKV("dog",make([]byte,5))
	wal.addKV("dog",make([]byte,5))
	wal.addKV("dog",make([]byte,5))
	wal.addKV("dog",make([]byte,5))
	wal.addKV("dog",make([]byte,5))
	wal.addKV("dog",make([]byte,5))
	wal.addKV("dog",make([]byte,5))
	wal.addKV("dog",make([]byte,5))
	wal.addKV("dog",make([]byte,5))
	wal.addKV("dog",make([]byte,5))
	wal.addKV("dog",make([]byte,5))
	fmt.Println(wal.readSegment(4))



}