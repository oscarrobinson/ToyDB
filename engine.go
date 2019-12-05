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
	binary.BigEndian.PutUint64(buffer[0:8], uint64(d.offset))
	binary.BigEndian.PutUint64(buffer[8:16], uint64(d.length))
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
	readBytes := recordLength
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
	Stat() (os.FileInfo, error)
}

type StorageEngineConfig struct {
	MapFilePath  string
	DataFilePath string
}

type StorageEngine struct {
	dataChannel             chan dataToWrite
	mapChannel              chan dataToMap
	offsetMap               map[[32]byte]dataInfo
	mapFile                 EngineFile
	mapFileLength           int64
	dataFile                EngineFile
	dataFileLength          int64
	shutdownTriggerChannel  chan struct{}
	shutdownResponseChannel chan int
}

func (eng *StorageEngine) Get(key string) ([]byte, error) {
	hash := sha256.Sum256([]byte(key))
	if keyDataInfo, ok := eng.offsetMap[hash]; ok {
		buffer := make([]byte, keyDataInfo.length)
		_, err := eng.dataFile.ReadAt(buffer, keyDataInfo.offset)
		return buffer, err
	} else {
		return nil, nil
	}
}

func (eng *StorageEngine) Shutdown() {
	eng.shutdownTriggerChannel <- shutdown{}
	mapShutdown := false
	dataShutdown := false
	for !(mapShutdown && dataShutdown) {
		entityShutdown := <-eng.shutdownResponseChannel
		if entityShutdown == dataProcessorShutDown {
			dataShutdown = true
		} else {
			mapShutdown = true
		}
	}
	close(eng.shutdownTriggerChannel)
	close(eng.shutdownResponseChannel)
	close(eng.dataChannel)
	close(eng.mapChannel)
	dataCloseErr := eng.dataFile.Close()
	if dataCloseErr != nil {
		log.Printf("Error closing data file: %s\n", dataCloseErr.Error())
	}
	mapCloseErr := eng.mapFile.Close()
	if mapCloseErr != nil {
		log.Printf("Error closing map file: %s\n", mapCloseErr.Error())
	}
}

func (eng *StorageEngine) processDataChannel() {
	for {
		select {
		case data := <-eng.dataChannel:
			bytesWritten, err := eng.dataFile.Write(data.value)
			offset := eng.dataFileLength
			eng.dataFileLength += int64(bytesWritten)
			if err != nil {
				log.Printf("Error writing data: %s\n", err.Error())
				data.responseChannel <- 1
			} else {
				info := dataInfo{offset, int64(bytesWritten)}
				mapData := dataToMap{data.key, info, data.responseChannel}
				eng.mapChannel <- mapData
			}
		case <-eng.shutdownTriggerChannel:
			eng.shutdownResponseChannel <- dataProcessorShutDown
			return
		default:
		}
	}
}

func (eng *StorageEngine) processMapChannel() {
	for {
		select {
		case mapInfo := <-eng.mapChannel:
			toWrite := append(mapInfo.key, mapInfo.dataInfo.toByteSlice()...)
			bytesWritten, err := eng.dataFile.Write(toWrite)
			eng.mapFileLength += int64(bytesWritten)
			if err != nil {
				log.Printf("Error writing map data: %s\n", err.Error())
				mapInfo.responseChannel <- 1
			} else {
				var key [32]byte
				copy(key[:], mapInfo.key[0:32])
				eng.offsetMap[key] = mapInfo.dataInfo
				mapInfo.responseChannel <- 0
			}
		case <-eng.shutdownTriggerChannel:
			eng.shutdownResponseChannel <- mapProcessorShutDown
			return
		default:
		}
	}
}

func NewStorageEngine(mapFile EngineFile, dataFile EngineFile) *StorageEngine {
	storageEngine := new(StorageEngine)
	storageEngine.dataChannel = make(chan dataToWrite)
	storageEngine.mapChannel = make(chan dataToMap)
	storageEngine.shutdownTriggerChannel = make(chan struct{})
	storageEngine.shutdownResponseChannel = make(chan int)
	storageEngine.offsetMap = parseOffsetMap(mapFile)
	storageEngine.mapFile = mapFile
	mapFileInfo, mapLengthErr := mapFile.Stat()
	if mapLengthErr != nil {
		log.Fatal("Failed to read map file length")
	}
	storageEngine.mapFileLength = mapFileInfo.Size()
	storageEngine.dataFile = dataFile
	dataFileInfo, dataLengthErr := dataFile.Stat()
	if dataLengthErr != nil {
		log.Fatal("Failed to read data file length")
	}
	storageEngine.dataFileLength = dataFileInfo.Size()
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
