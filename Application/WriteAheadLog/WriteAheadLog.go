package WriteAheadLog

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

type Line struct {
	Crc       uint32
	Timestamp uint64
	Tombstone byte
	Keysize   uint64
	Valuesize uint64
	Key       string
	Value     []byte
}

type WriteAheadLog struct {
	file    string
	Data    []Line
	segLoc  []int
	segment int64
	LWM     int
}

func ParseLine(f *os.File) Line {
	crc := make([]byte, 4)
	timestamp := make([]byte, 8)
	tombstone := make([]byte, 1)
	keysize := make([]byte, 8)
	valuesize := make([]byte, 8)
	line := Line{}
	_, _ = f.Read(crc)
	crcint := binary.LittleEndian.Uint32(crc)
	line.Crc = crcint
	_, _ = f.Read(timestamp)
	timestampint := binary.LittleEndian.Uint64(timestamp)
	line.Timestamp = timestampint
	_, _ = f.Read(tombstone)
	line.Tombstone = tombstone[0]
	_, _ = f.Read(keysize)
	keysizeint := binary.LittleEndian.Uint64(keysize)
	line.Keysize = keysizeint
	_, _ = f.Read(valuesize)
	valuesizeint := binary.LittleEndian.Uint64(valuesize)
	line.Valuesize = valuesizeint
	key := make([]byte, keysizeint)
	value := make([]byte, valuesizeint)
	_, _ = f.Read(key)
	_, _ = f.Read(value)
	line.Key = string(key)
	line.Value = value
	return line
}

func SerializeLine(line Line) []byte {
	var allbytes []byte
	mybytes := []byte(line.Key)
	for i := 0; i < len(line.Value); i++ {
		mybytes = append(mybytes, line.Value[i])
	}
	chcks := CRC32(mybytes)
	chcksbytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(chcksbytes, chcks)

	t := time.Now().Unix()
	fulltimestamp := uint64(t)
	fulltimestampbytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(fulltimestampbytes, fulltimestamp)

	tombstone := byte(0)

	keysize := uint64(len(line.Key))
	keysizebytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(keysizebytes, keysize)

	valuesize := uint64(len(line.Value))
	valuesizebytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(valuesizebytes, valuesize)

	keybytes := []byte(line.Key)

	allbytes = append(allbytes, chcksbytes...)
	allbytes = append(allbytes, fulltimestampbytes...)
	allbytes = append(allbytes, tombstone)
	allbytes = append(allbytes, keysizebytes...)
	allbytes = append(allbytes, valuesizebytes...)
	allbytes = append(allbytes, keybytes...)
	allbytes = append(allbytes, line.Value...)

	return allbytes
}

func (wal *WriteAheadLog) Init(segment int64) {
	wal.segment = segment
	f, err := os.Open("wal")
	if err != nil {
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

	_, err = f.Readdirnames(2)
	if err == io.EOF {
		file, err := os.Create("wal/wal-0001.log")
		if err != nil {
			panic("File already exists")
		}
		wal.file = "wal/wal-0001.log"
		wal.segLoc = append(wal.segLoc, 1)
		err = file.Close()
		if err != nil {
			return
		}
		return
	}
	wal.read()
}

func (wal *WriteAheadLog) read() {
	files, err := ioutil.ReadDir("wal/")
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if strings.Contains(file.Name(), "wal") {
			sliced := strings.Split(file.Name(), ".")
			sliced2 := strings.Split(sliced[0], "wal-")
			intval, _ := strconv.Atoi(sliced2[1])
			wal.segLoc = append(wal.segLoc, intval)
		}
	}
	lastindex := wal.segLoc[len(wal.segLoc)-1]
	pad := fmt.Sprintf("%04d", lastindex)
	filename := "wal/wal-" + pad + ".log"
	wal.file = filename
	wal.Data = wal.readSegment(lastindex)

}

func (wal *WriteAheadLog) readSegment(segment int) []Line {
	pad := fmt.Sprintf("%04d", segment)
	filename := "wal/wal-" + pad + ".log"
	f, err := os.Open(filename)
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {

		}
	}(f)
	if err != nil {
		panic("err")
	}
	var data []Line
	for i := 1; int64(i) <= wal.segment; i++ {
		data = append(data, ParseLine(f))
		if errors.Is(err, io.EOF) {
			return data
		}

	}
	return data
}

func (wal *WriteAheadLog) LowWaterMarkRemoval() {
	files, err := ioutil.ReadDir("wal/")
	if err != nil {
		log.Fatal(err)
	}

	if wal.LWM == -1 {
		return
	}
	if wal.LWM == 0 {
		todelete := wal.segLoc
		for elem := range todelete {
			pad := fmt.Sprintf("%04d", todelete[elem])
			filename := "wal/wal-" + pad + ".log"
			err := os.Remove(filename)
			if err != nil {
			}
		}
		wal.segLoc = nil
		wal.Data = nil
		return
	}
	if wal.LWM >= len(files) {
		return
	}
	todelete := wal.segLoc[:len(wal.segLoc)-wal.LWM]
	toremain := wal.segLoc[len(wal.segLoc)-wal.LWM:]
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

func (wal *WriteAheadLog) AddKV(key string, value []byte) bool {
	if int64(len(wal.Data)) >= wal.segment {
		i := wal.segLoc[len(wal.segLoc)-1] + 1
		wal.segLoc = append(wal.segLoc, i)
		pad := fmt.Sprintf("%04d", i)
		filename := "wal/wal-" + pad + ".log"
		file, err := os.Create(filename)
		if err != nil {
			panic("File already exists")
		}
		wal.Data = nil
		wal.file = filename
		err = file.Close()
		if err != nil {
			return false
		}
	}
	myfile, err := os.OpenFile(wal.file, os.O_APPEND, 0777)
	defer func(myfile *os.File) {
		err := myfile.Close()
		if err != nil {

		}
	}(myfile)
	if err != nil {
		return false
	}
	mybytes := []byte(key)
	for i := 0; i < len(value); i++ {
		mybytes = append(mybytes, value[i])
	}
	var allbytes []byte
	chcks := CRC32(mybytes)
	chcksbytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(chcksbytes, chcks)

	t := time.Now().Unix()
	fulltimestamp := uint(t)
	fulltimestampbytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(fulltimestampbytes, uint64(fulltimestamp))

	tombstone := byte(0)

	keysize := uint64(len(key))
	keysizebytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(keysizebytes, keysize)

	valuesize := uint64(len(value))
	valuesizebytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(valuesizebytes, valuesize)

	keybytes := []byte(key)

	allbytes = append(allbytes, chcksbytes...)
	allbytes = append(allbytes, fulltimestampbytes...)
	allbytes = append(allbytes, tombstone)
	allbytes = append(allbytes, keysizebytes...)
	allbytes = append(allbytes, valuesizebytes...)
	allbytes = append(allbytes, keybytes...)
	allbytes = append(allbytes, value...)
	line := Line{chcks, uint64(fulltimestamp), tombstone, keysize, valuesize, key, value}
	wal.Data = append(wal.Data, line)
	_, err = myfile.Write(allbytes)
	if err != nil {
		return false
	}

	return true

}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
