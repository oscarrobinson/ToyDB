package toydb

import (
	"crypto/sha256"
	"encoding/binary"
	"io"
	"log"
	"os"
)

const (
	mapProcessorShutDown  = 1
	dataProcessorShutDown = 2
)

type shutdown struct{}

type dataInfo struct {
	offset int64
	length int64
}

func (d dataInfo) toByteSlice() []byte {
	buffer := make([]byte, 16)
	binary.BigEndian.PutUnint64(buffer[0:8], uint64(dataInfo.offset))
	binary.BigEndian.PutUnint64(buffer[8:16], uint64(dataInfo.offset))
	return buffer
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
	dataInfo        dataInfo
	responseChannel chan int
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
	Stat() (FileInfo, error)
}

type StorageEngineConfig struct {
	MapFilePath  string
	DataFilePath string
}

type StorageEngine struct {
	dataChannel      chan []dataToWrite
	mapChannel       chan []dataToMap
	offsetMap        map[[32]byte]dataInfo
	mapFile          EngineFile
	mapFileLength    int64
	dataFile         EngineFile
	dataFileLength   int64
	shutdownTrigger  chan []struct{}
	shutdownResponse chan int
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
	eng.shutdownTrigger <- shutdown
	mapShutdown := false
	dataShutdown := false
	for !(mapShutdown && dataShutdown) {
		entityShutdown <- eng.shutdownResponse
		if entityShutdown == dataProcessorShutDown {
			dataShutdown = true
		} else {
			mapShutdown = true
		}
	}
	close(eng.shutdownTrigger)
	close(eng.shutdownResponse)
	close(eng.dataChannel)
	close(eng.mapChannel)
	dataCloseErr := eng.dataFile.Close()
	if dataCloseErr != nil {
		log.Println("Error closing data file: %s", dataCloseErr.Error())
	}
	mapCloseErr := eng.mapFile.Close()
	if mapCloseErr != nil {
		log.Println("Error closing map file: %s", mapCloseErr.Error())
	}
}

func (eng StorageEngine) processDataChannel() {
	for {
		select {
		case writeData <- eng.dataChannel:
			bytesWritten, err := eng.dataFile.Write(writeData.value)
			eng.dataFileLength += bytesWritten
			if err != nil {
				log.Println("Error writing data: %s", err.Error())
				writeData.responseChannel <- 1
			} else {
				info := dataInfo{eng.dataFile.length, bytesWritten}
				mapData := dataToMap{writeData.key, info, writeData.responseChannel}
				eng.mapChannel <- mapData
			}
		case <-eng.shutdownTrigger:
			eng.shutdownResponse <- dataProcessorShutDown
			return
		default:
		}
	}
}

func (eng StorageEngine) processMapChannel() {
	for {
		select {
		case mapData <- eng.mapChannel:
			toWrite := append(mapData, mapData.dataInfo.toByteSlice()...)
			bytesWritten, err := eng.dataFile.Write(toWrite)
			eng.mapFileLength += bytesWritten
			if err != nil {
				log.Println("Error writing map data: %s", err.Error())
				writeData.responseChannel <- 1
			} else {
				eng.offsetMap[mapData] = mapData.dataInfo
				writeData.responseChannel <- 0
			}
		case <-eng.shutdownTrigger:
			eng.shutdownResponse <- mapProcessorShutDown
			return
		default:
		}
	}
}

func NewStorageEngine(mapFile EngineFile, dataFile EngineFile) *StorageEngine {
	storageEngine := new(StorageEngine)
	storageEngine.dataChannel = make(chan []dataToWrite)
	storageEngine.mapChannel = make(chan []dataToMap)
	storageEngine.shutdownTrigger = make(chan struct{})
	storageEngine.shutdownResponse = make(chan int)
	storageEngine.offsetMap = parseOffsetMap(mapFile)
	storageEngine.mapFile = mapFile
	mapFileLength, mapLengthErr = mapFile.Stat()
	if mapLengthErr != nil {
		log.Fatal("Failed to read map file length")
	}
	storageEngine.mapFileLength = mapFileLength
	storageEngine.dataFile = dataFile
	dataFileLength, dataLengthErr = dataFile.Stat()
	if dataLengthErr != nil {
		log.Fatal("Failed to read data file length")
	}
	storageEngine.dataFileLength = dataFileLength
	go storageEngine.processDataChannel()
	go storageEngine.processMapChannel()
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
// - Test processDataChannel
// - Test processMapChannel
// - Test Shutdown
// = Test NewStorageEngine
// = Test toByteSlice
// - Implement the Set function
// - Test Set function

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
