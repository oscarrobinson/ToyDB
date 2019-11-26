package toydb

import (
	"encoding/binary"
	"io"
	"log"
	"os"
	"crypto/sha256"
)

type dataInfo struct {
	offset int64
	length int64
}

//32 byte key hash, 8 byte uint64 for offset, 8 byte uint64 for length
func parseOffsetMap(file io.ReaderAt) map[[32]byte]dataInfo {
	const recordLength = 48
	var offset int64 = 0
	offsetMap := make(map[[32]byte]dataInfo)
	var hashBuffer [recordLength]byte
	var dataOffsetIntBytes [8]byte
	var dataLengthIntBytes [8]byte
	var readBytes int = recordLength
	for readBytes == recordLength {
		var err error
		readBytes, err = file.ReadAt(hashBuffer[:], offset)
		if readBytes == recordLength {
			if err != io.EOF && err != nil {
				log.Fatal("Failed to parse header")
			}
			var keyHash [32]byte
			copy(keyHash[:], hashBuffer[:])
			copy(dataOffsetIntBytes[:], hashBuffer[32:40])
			copy(dataLengthIntBytes[:], hashBuffer[40:48])
			dataOffsetInt := int64(binary.BigEndian.Uint64(dataOffsetIntBytes[:]))
			dataLengthInt := int64(binary.BigEndian.Uint64(dataLengthIntBytes[:]))
			offsetMap[keyHash] = dataInfo{offset: dataOffsetInt, length: dataLengthInt}
			offset += int64(readBytes)
		}
	}
	return offsetMap
}

type dataToWrite struct {
	key             []byte
	value           []byte
	responseChannel chan int
}

type dataToMap struct {
	key             []byte
	value           []byte
	responseChannel chan int
	dataInfo        dataInfo
}

func writeData(requestId string, file io.Writer, data []byte) (int, error) {
	writtenBytesCount, err := file.Write(data)
	if err != nil {
		log.Printf("[%s] Error writing data for request: %v", requestId, err)
		return 0, err
	}
	return writtenBytesCount, nil
}

// methods on storage engine struct
// set - sends data to write channel
//	   - write channel writes the data to the file
// get - fetches value for hash

type EngineFile interface {
	io.ReaderAt
	io.Writer
	io.Closer
}

type StorageEngineConfig struct {
	MapFilePath  string
	DataFilePath string
}

type StorageEngine struct {
	dataChannel chan []dataToWrite
	mapChannel  chan []dataToMap
	offsetMap   map[[32]byte]dataInfo
	mapFile     EngineFile
	dataFile    EngineFile
}

func (eng StorageEngine) Get(key string) ([]byte, error) {
	hash := sha256.Sum256([]byte(key))
	if keyDataInfo, ok := eng.offsetMap[hash]; ok {
	    buffer := make([]byte, keyDataInfo.length)
		_, err := eng.dataFile.ReadAt(buffer, keyDataInfo.offset)
		return buffer, err
	} else {
		return nil, nil
	}
}

func (eng StorageEngine) Shutdown() {
	close(eng.dataChannel)
	close(eng.mapChannel)
	dataCloseErr := eng.dataFile.Close()
	if dataCloseErr != nil {
		log.Println("Error closing data file")
	}
	mapCloseErr := eng.mapFile.Close()
	if mapCloseErr != nil {
		log.Println("Error closing map file")
	}
}

func NewStorageEngine(mapFile EngineFile, dataFile EngineFile) *StorageEngine {
	storageEngine := new(StorageEngine)
	storageEngine.dataChannel = make(chan []dataToWrite)
	storageEngine.mapChannel = make(chan []dataToMap)
	storageEngine.offsetMap = parseOffsetMap(mapFile)
	storageEngine.mapFile = mapFile
	storageEngine.dataFile = dataFile
	return storageEngine
}

func OpenFile(path string) *os.File {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		log.Fatal("Failed to open data file")
	}
	return file
}

//TODO
// - Failure cases for storage engine get tests
// - Write initializer that starts loops reading the data and map channels
// - Implement the Set function
// - Tests

// func (n StorageEngine) Set(key string, value string) *error {
// 	responseChannel := make(chan int)
// 	writeData := dataToWrite{
// 		key: sha256.Sum256([]byte(key)),
// 		value: []byte(value),
// 		responseChannel
// 	}
// 	n.dataChannel <- writeData
// 	result <- responseChannel
// 	if (result == 1) {
// 		return nil
// 	} else {
// 		return x
// 	}

// }
