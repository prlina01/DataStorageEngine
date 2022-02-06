package main

import (
	"encoding/binary"
	"errors"
	"fmt"
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
	wal.segment = segment
	f,err := os.Open("wal")
	if err != nil{
		_, err := os.Create("wal")
		if err != nil {
			return
		}
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {

		}
	}(f)

	_,err = f.Readdirnames(1)
	if err == io.EOF{
		file,err := os.Create("wal/wal-0001.log")
		if err!=nil {
			panic("File already exists")
		}
		wal.file = "wal/wal-0001.log"
		wal.segLoc = append(wal.segLoc,1)
		err = file.Close()
		if err != nil {
			return
		}
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
	wal.data = wal.readSegment(lastindex)

}

func (wal *WriteAheadLog) readSegment(segment int) []Line {
	pad := fmt.Sprintf("%04d",segment)
	filename := "wal/wal-"+pad+".log"
	f, err := os.Open(filename)
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {

		}
	}(f)
	if err!=nil {
		panic("err")
	}
	var data []Line
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

func (wal *WriteAheadLog) lowWaterMarkRemoval(LWM int){
	files, err := ioutil.ReadDir("wal/")
	if err != nil {
		log.Fatal(err)
	}

	if LWM == -1{
		return
	}
	if LWM == 0{
		todelete := wal.segLoc
		for elem := range todelete {
			pad := fmt.Sprintf("%04d", todelete[elem])
			filename := "wal/wal-" + pad + ".log"
			err := os.Remove(filename)
			if err != nil {
			}
		}
		wal.segLoc = nil
		wal.data = nil
		return
	}
	if LWM >= len(files){
		return
	}
	todelete := wal.segLoc[:len(wal.segLoc)-LWM]
	toremain := wal.segLoc[len(wal.segLoc)-LWM:]
	for elem := range todelete {
		pad := fmt.Sprintf("%04d", todelete[elem])
		filename := "wal/wal-" + pad + ".log"
		err := os.Remove(filename)
		if err != nil {
			return
		}
		wal.segLoc = toremain
	}
}

func (wal *WriteAheadLog) addKV(key string,value []byte){
	if int64(len(wal.data)) >= wal.segment{
		i := wal.segLoc[len(wal.segLoc)-1]+1
		wal.segLoc = append(wal.segLoc,i)
		pad := fmt.Sprintf("%04d",i)
		filename := "wal/wal-"+pad+".log"
		file,err := os.Create(filename)
		if err!=nil {
			panic("File already exists")
		}
		wal.data = nil
		wal.file = filename
		err = file.Close()
		if err != nil {
			return
		}
	}
	myfile,err := os.OpenFile(wal.file,os.O_APPEND,0777)
	defer func(myfile *os.File) {
		err := myfile.Close()
		if err != nil {

		}
	}(myfile)
	if err!=nil{
		panic("err")
	}
	mybytes := []byte(key)
	for i:=0;i< len(value);i++{
	mybytes = append(mybytes,value[i])
	}
	var allbytes []byte
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
	_, err = myfile.Write(allbytes)
	if err != nil {
		return
	}


}


func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
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
	wal.lowWaterMarkRemoval(1)




}