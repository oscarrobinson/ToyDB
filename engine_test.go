package toydb

import (
	"bytes"
	"errors"
	"io"
	"os"
	"reflect"
	"testing"
)

func shouldEqual(t *testing.T, got interface{}, expected interface{}) {
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("%d did not equal expected %d", got, expected)
	}
}

func Test_parseOffsetMap_returnsCorrectMapWhenOneKey(t *testing.T) {
	fileContents := [48]byte{
		0x07, 0x7F, 0x33, 0x77, 0xC2, 0xE9, 0xAE, 0xD3,
		0x2C, 0xBA, 0xE1, 0xA9, 0xCC, 0x2C, 0x65, 0xDA,
		0x3E, 0x3D, 0xD4, 0x58, 0xCF, 0x14, 0x04, 0xE1,
		0xFB, 0xC6, 0xCD, 0x29, 0x75, 0x95, 0x37, 0xE6,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7B,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09}

	expectedKey := [32]byte{
		0x07, 0x7F, 0x33, 0x77, 0xC2, 0xE9, 0xAE, 0xD3,
		0x2C, 0xBA, 0xE1, 0xA9, 0xCC, 0x2C, 0x65, 0xDA,
		0x3E, 0x3D, 0xD4, 0x58, 0xCF, 0x14, 0x04, 0xE1,
		0xFB, 0xC6, 0xCD, 0x29, 0x75, 0x95, 0x37, 0xE6}

	var expectedValue dataInfo = dataInfo{123, 9}

	expectedMap := map[[32]byte]dataInfo{expectedKey: expectedValue}

	offsetMap := parseOffsetMap(bytes.NewReader(fileContents[:]))

	shouldEqual(t, expectedMap, offsetMap)
}

func Test_parseOffsetMap_returnsCorrectMapWhenTwoKeys(t *testing.T) {
	fileContents := [96]byte{
		0x07, 0x7F, 0x33, 0x77, 0xC2, 0xE9, 0xAE, 0xD3,
		0x2C, 0xBA, 0xE1, 0xA9, 0xCC, 0x2C, 0x65, 0xDA,
		0x3E, 0x3D, 0xD4, 0x58, 0xCF, 0x14, 0x04, 0xE1,
		0xFB, 0xC6, 0xCD, 0x29, 0x75, 0x95, 0x37, 0xE6,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7B,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,
		0x17, 0x7F, 0x33, 0x77, 0xC2, 0xE9, 0xAE, 0xD3,
		0x2C, 0xBA, 0xE1, 0xA9, 0xCC, 0x2C, 0x65, 0xDA,
		0x3E, 0x3D, 0xD4, 0x58, 0xCF, 0x14, 0x04, 0xE1,
		0xFB, 0xC6, 0xCD, 0x29, 0x75, 0x95, 0x37, 0xE6,
		0x00, 0x00, 0x02, 0x22, 0xE1, 0x59, 0xE7, 0xB2,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F}

	expectedKey := [32]byte{
		0x07, 0x7F, 0x33, 0x77, 0xC2, 0xE9, 0xAE, 0xD3,
		0x2C, 0xBA, 0xE1, 0xA9, 0xCC, 0x2C, 0x65, 0xDA,
		0x3E, 0x3D, 0xD4, 0x58, 0xCF, 0x14, 0x04, 0xE1,
		0xFB, 0xC6, 0xCD, 0x29, 0x75, 0x95, 0x37, 0xE6}

	expectedKey2 := [32]byte{
		0x17, 0x7F, 0x33, 0x77, 0xC2, 0xE9, 0xAE, 0xD3,
		0x2C, 0xBA, 0xE1, 0xA9, 0xCC, 0x2C, 0x65, 0xDA,
		0x3E, 0x3D, 0xD4, 0x58, 0xCF, 0x14, 0x04, 0xE1,
		0xFB, 0xC6, 0xCD, 0x29, 0x75, 0x95, 0x37, 0xE6}

	var expectedValue dataInfo = dataInfo{123, 9}
	var expectedValue2 dataInfo = dataInfo{2348832909234, 15}

	expectedMap := map[[32]byte]dataInfo{expectedKey: expectedValue, expectedKey2: expectedValue2}

	offsetMap := parseOffsetMap(bytes.NewReader(fileContents[:]))

	shouldEqual(t, expectedMap, offsetMap)
}

func Test_parseOffsetMap_returnsMapWithLastValueWhenDuplicateKeys(t *testing.T) {
	fileContents := [96]byte{
		0x07, 0x7F, 0x33, 0x77, 0xC2, 0xE9, 0xAE, 0xD3,
		0x2C, 0xBA, 0xE1, 0xA9, 0xCC, 0x2C, 0x65, 0xDA,
		0x3E, 0x3D, 0xD4, 0x58, 0xCF, 0x14, 0x04, 0xE1,
		0xFB, 0xC6, 0xCD, 0x29, 0x75, 0x95, 0x37, 0xE6,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7B,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,
		0x07, 0x7F, 0x33, 0x77, 0xC2, 0xE9, 0xAE, 0xD3,
		0x2C, 0xBA, 0xE1, 0xA9, 0xCC, 0x2C, 0x65, 0xDA,
		0x3E, 0x3D, 0xD4, 0x58, 0xCF, 0x14, 0x04, 0xE1,
		0xFB, 0xC6, 0xCD, 0x29, 0x75, 0x95, 0x37, 0xE6,
		0x00, 0x00, 0x02, 0x22, 0xE1, 0x59, 0xE7, 0xB2,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F}

	expectedKey := [32]byte{
		0x07, 0x7F, 0x33, 0x77, 0xC2, 0xE9, 0xAE, 0xD3,
		0x2C, 0xBA, 0xE1, 0xA9, 0xCC, 0x2C, 0x65, 0xDA,
		0x3E, 0x3D, 0xD4, 0x58, 0xCF, 0x14, 0x04, 0xE1,
		0xFB, 0xC6, 0xCD, 0x29, 0x75, 0x95, 0x37, 0xE6}

	var expectedValue dataInfo = dataInfo{2348832909234, 15}

	expectedMap := map[[32]byte]dataInfo{expectedKey: expectedValue}

	offsetMap := parseOffsetMap(bytes.NewReader(fileContents[:]))

	shouldEqual(t, expectedMap, offsetMap)
}

func Test_writeData_writesAndReturnsBytesWrittenCount(t *testing.T) {
	var buffer bytes.Buffer
	dataToWrite := [8]byte{0x07, 0x7F, 0x33, 0x77, 0xC2, 0xE9, 0xAE, 0xD3}
	bytesWritten, err := writeData("RQ-1", &buffer, dataToWrite[:])

	if err != nil {
		t.Errorf("err: %d did not equal expected nil", err)
	}

	if bytesWritten != 8 {
		t.Errorf("bytesWritten: %d did not equal expected %d", bytesWritten, 8)
	}

	shouldEqual(t, buffer.Bytes(), dataToWrite[:])
}

type writerStub struct {
}

func (c writerStub) Write(data []byte) (int, error) {
	return 0, errors.New("Broken")
}

func Test_writeData_returnErrorWhenWriteErrors(t *testing.T) {
	writer := writerStub{}

	dataToWrite := [8]byte{0x07, 0x7F, 0x33, 0x77, 0xC2, 0xE9, 0xAE, 0xD3}
	bytesWritten, err := writeData("RQ-1", &writer, dataToWrite[:])

	shouldEqual(t, err.Error(), "Broken")
	shouldEqual(t, bytesWritten, 0)
}

type mockWriteEngineFile struct {
	io.Writer
}

func (mockWriteEngineFile) Close() error                                  { return nil }
func (mockWriteEngineFile) Stat() (os.FileInfo, error)                    { return nil, nil }
func (mockWriteEngineFile) ReadAt(p []byte, off int64) (n int, err error) { return 0, nil }

type mockReadEngineFile struct {
	io.ReaderAt
}

func (mockReadEngineFile) Close() error                      { return nil }
func (mockReadEngineFile) Stat() (os.FileInfo, error)        { return nil, nil }
func (mockReadEngineFile) Write(p []byte) (n int, err error) { return 0, nil }

type mockErroredWriteEngineFile struct{}

func (mockErroredWriteEngineFile) Write(p []byte) (n int, err error)             { return 2, errors.New("Broken") }
func (mockErroredWriteEngineFile) Close() error                                  { return nil }
func (mockErroredWriteEngineFile) Stat() (os.FileInfo, error)                    { return nil, nil }
func (mockErroredWriteEngineFile) ReadAt(p []byte, off int64) (n int, err error) { return 0, nil }

func Test_StorageEngine_Get_returnsValueForKey(t *testing.T) {
	dataFileContents := [48]byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}

	sha256Key := [32]byte{
		0x52, 0xec, 0x14, 0x80, 0x30, 0x8a, 0x78, 0xb9,
		0xf1, 0xc9, 0xc7, 0xd9, 0x65, 0x4d, 0x4a, 0x85,
		0x81, 0x9c, 0x26, 0xb3, 0x3a, 0x32, 0xf5, 0xc2,
		0x7d, 0x47, 0x2a, 0x46, 0x2f, 0x10, 0x2f, 0x45}

	storageEngine := new(StorageEngine)
	storageEngine.dataFile = mockReadEngineFile{bytes.NewReader(dataFileContents[:])}
	storageEngine.offsetMap = make(map[[32]byte]dataInfo)
	storageEngine.offsetMap[sha256Key] = dataInfo{3, 5}

	result, err := storageEngine.Get("randomkey")

	expectedResult := [5]byte{0x04, 0x05, 0x06, 0x07, 0x08}

	shouldEqual(t, result, expectedResult[:])
	shouldEqual(t, err, nil)
}

func Test_StorageEngine_Get_returnsNilWhenKeyNotFound(t *testing.T) {
	storageEngine := new(StorageEngine)
	storageEngine.offsetMap = make(map[[32]byte]dataInfo)

	result, err := storageEngine.Get("randomkey")

	t.Log(result == nil)

	if result != nil {
		t.Errorf("%d did not equal expected nil", result)
	}
	if err != nil {
		t.Errorf("%d did not equal expected nil", err)
	}
}

func Test_StorageEngine_Get_returnsErrorWhenReadFails(t *testing.T) {
	dataFileContents := [48]byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}

	sha256Key := [32]byte{
		0x52, 0xec, 0x14, 0x80, 0x30, 0x8a, 0x78, 0xb9,
		0xf1, 0xc9, 0xc7, 0xd9, 0x65, 0x4d, 0x4a, 0x85,
		0x81, 0x9c, 0x26, 0xb3, 0x3a, 0x32, 0xf5, 0xc2,
		0x7d, 0x47, 0x2a, 0x46, 0x2f, 0x10, 0x2f, 0x45}

	storageEngine := new(StorageEngine)
	storageEngine.dataFile = mockReadEngineFile{bytes.NewReader(dataFileContents[:])}
	storageEngine.offsetMap = make(map[[32]byte]dataInfo)
	storageEngine.offsetMap[sha256Key] = dataInfo{3, 100}

	_, err := storageEngine.Get("randomkey")

	shouldEqual(t, err.Error(), "EOF")
}

func Test_StorageEngine_processDataChannel_writesDataAndSendsInfoToMapChannel(t *testing.T) {
	storageEngine := new(StorageEngine)
	var buf bytes.Buffer
	storageEngine.dataFile = mockWriteEngineFile{&buf}
	storageEngine.dataChannel = make(chan dataToWrite)
	storageEngine.mapChannel = make(chan dataToMap)
	storageEngine.shutdownTriggerChannel = make(chan struct{})
	storageEngine.shutdownResponseChannel = make(chan int)

	go storageEngine.processDataChannel()

	key := [32]byte{
		0x07, 0x7F, 0x33, 0x77, 0xC2, 0xE9, 0xAE, 0xD3,
		0x2C, 0xBA, 0xE1, 0xA9, 0xCC, 0x2C, 0x65, 0xDA,
		0x3E, 0x3D, 0xD4, 0x58, 0xCF, 0x14, 0x04, 0xE1,
		0xFB, 0xC6, 0xCD, 0x29, 0x75, 0x95, 0x37, 0xE6}

	value := [5]byte{0x01, 0x02, 0x03, 0x04, 0x05}

	responseChannel := make(chan int)
	storageEngine.dataChannel <- dataToWrite{key[:], value[:], responseChannel}
	mapData := <-storageEngine.mapChannel

	shouldEqual(t, buf.Bytes(), value[:])
	shouldEqual(t, storageEngine.dataFileLength, int64(5))
	shouldEqual(t, mapData, dataToMap{key[:], dataInfo{0, 5}, responseChannel})
}

func Test_StorageEngine_processDataChannel_sendsToResponseChannelOnErr(t *testing.T) {
	storageEngine := new(StorageEngine)
	storageEngine.dataFile = mockErroredWriteEngineFile{}
	storageEngine.dataChannel = make(chan dataToWrite)
	storageEngine.mapChannel = make(chan dataToMap)
	storageEngine.shutdownTriggerChannel = make(chan struct{})
	storageEngine.shutdownResponseChannel = make(chan int)

	go storageEngine.processDataChannel()

	key := [32]byte{
		0x07, 0x7F, 0x33, 0x77, 0xC2, 0xE9, 0xAE, 0xD3,
		0x2C, 0xBA, 0xE1, 0xA9, 0xCC, 0x2C, 0x65, 0xDA,
		0x3E, 0x3D, 0xD4, 0x58, 0xCF, 0x14, 0x04, 0xE1,
		0xFB, 0xC6, 0xCD, 0x29, 0x75, 0x95, 0x37, 0xE6}

	value := [5]byte{0x01, 0x02, 0x03, 0x04, 0x05}

	responseChannel := make(chan int)
	storageEngine.dataChannel <- dataToWrite{key[:], value[:], responseChannel}
	res := <-responseChannel

	shouldEqual(t, res, 1)
	//Should still update length for whatever number of bytes did get written to the data file
	shouldEqual(t, storageEngine.dataFileLength, int64(2))
}

func Test_StorageEngine_processDataChannel_shouldReturnWhenShutdownTriggerReceived(t *testing.T) {
	storageEngine := new(StorageEngine)
	storageEngine.dataFile = mockErroredWriteEngineFile{}
	storageEngine.dataChannel = make(chan dataToWrite)
	storageEngine.mapChannel = make(chan dataToMap)
	storageEngine.shutdownTriggerChannel = make(chan struct{})
	storageEngine.shutdownResponseChannel = make(chan int)

	go storageEngine.processDataChannel()
	storageEngine.shutdownTriggerChannel <- shutdown{}
	shutdownRes := <-storageEngine.shutdownResponseChannel
	shouldEqual(t, shutdownRes, dataProcessorShutDown)
}

func Test_StorageEngine_processMapChannel_writesMapDataAndUpdatesMap(t *testing.T) {
	storageEngine := new(StorageEngine)
	var buf bytes.Buffer
	storageEngine.mapFile = mockWriteEngineFile{&buf}
	storageEngine.mapChannel = make(chan dataToMap)
	storageEngine.shutdownTriggerChannel = make(chan struct{})
	storageEngine.shutdownResponseChannel = make(chan int)
	storageEngine.offsetMap = make(map[[32]byte]dataInfo)
	responseChannel := make(chan int)

	key := [32]byte{
		0x07, 0x7F, 0x33, 0x77, 0xC2, 0xE9, 0xAE, 0xD3,
		0x2C, 0xBA, 0xE1, 0xA9, 0xCC, 0x2C, 0x65, 0xDA,
		0x3E, 0x3D, 0xD4, 0x58, 0xCF, 0x14, 0x04, 0xE1,
		0xFB, 0xC6, 0xCD, 0x29, 0x75, 0x95, 0x37, 0xE6}

	expectedWrittenData := [48]byte{
		0x07, 0x7F, 0x33, 0x77, 0xC2, 0xE9, 0xAE, 0xD3,
		0x2C, 0xBA, 0xE1, 0xA9, 0xCC, 0x2C, 0x65, 0xDA,
		0x3E, 0x3D, 0xD4, 0x58, 0xCF, 0x14, 0x04, 0xE1,
		0xFB, 0xC6, 0xCD, 0x29, 0x75, 0x95, 0x37, 0xE6,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0C,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F}

	info := dataInfo{12, 15}
	go storageEngine.processMapChannel()
	storageEngine.mapChannel <- dataToMap{key[:], info, responseChannel}

	shouldEqual(t, <-responseChannel, 0)
	shouldEqual(t, storageEngine.mapFileLength, int64(48))
	shouldEqual(t, buf.Bytes(), expectedWrittenData[:])
	shouldEqual(t, storageEngine.offsetMap[key], info)
}

func Test_StorageEngine_processMapChannel_sendsOneToResponseChannelOnError(t *testing.T) {
	storageEngine := new(StorageEngine)
	storageEngine.mapFile = mockErroredWriteEngineFile{}
	storageEngine.mapChannel = make(chan dataToMap)
	storageEngine.shutdownTriggerChannel = make(chan struct{})
	storageEngine.shutdownResponseChannel = make(chan int)
	storageEngine.offsetMap = make(map[[32]byte]dataInfo)
	responseChannel := make(chan int)

	key := [32]byte{
		0x07, 0x7F, 0x33, 0x77, 0xC2, 0xE9, 0xAE, 0xD3,
		0x2C, 0xBA, 0xE1, 0xA9, 0xCC, 0x2C, 0x65, 0xDA,
		0x3E, 0x3D, 0xD4, 0x58, 0xCF, 0x14, 0x04, 0xE1,
		0xFB, 0xC6, 0xCD, 0x29, 0x75, 0x95, 0x37, 0xE6}

	info := dataInfo{12, 15}
	go storageEngine.processMapChannel()
	storageEngine.mapChannel <- dataToMap{key[:], info, responseChannel}

	shouldEqual(t, <-responseChannel, 1)
	shouldEqual(t, storageEngine.mapFileLength, int64(2))
	_, ok := storageEngine.offsetMap[key]
	shouldEqual(t, ok, false)
}

func Test_StorageEngine_processMapChannel_shouldReturnWhenShutdownTriggerReceived(t *testing.T) {
	storageEngine := new(StorageEngine)
	storageEngine.dataChannel = make(chan dataToWrite)
	storageEngine.mapChannel = make(chan dataToMap)
	storageEngine.shutdownTriggerChannel = make(chan struct{})
	storageEngine.shutdownResponseChannel = make(chan int)

	go storageEngine.processMapChannel()
	storageEngine.shutdownTriggerChannel <- shutdown{}
	shutdownRes := <-storageEngine.shutdownResponseChannel
	shouldEqual(t, shutdownRes, mapProcessorShutDown)
}

func Test_StorageEngine_Shutdown_shouldReturnOnceProcessorsShutdown(t *testing.T) {
	storageEngine := new(StorageEngine)
	storageEngine.dataFile = mockReadEngineFile{}
	storageEngine.mapFile = mockReadEngineFile{}
	storageEngine.dataChannel = make(chan dataToWrite)
	storageEngine.mapChannel = make(chan dataToMap)
	storageEngine.shutdownTriggerChannel = make(chan struct{})
	storageEngine.shutdownResponseChannel = make(chan int)

	go storageEngine.Shutdown()
	<-storageEngine.shutdownTriggerChannel

	storageEngine.shutdownResponseChannel <- mapProcessorShutDown
	storageEngine.shutdownResponseChannel <- dataProcessorShutDown

	_, ok := <-storageEngine.mapChannel
	shouldEqual(t, ok, false)
}

func Test_StorageEngine_Set_shouldReturnNoErrorWhenSuccessOnResponseChannel(t *testing.T) {
	storageEngine := new(StorageEngine)
	storageEngine.dataFile = mockReadEngineFile{}
	storageEngine.mapFile = mockReadEngineFile{}
	storageEngine.dataChannel = make(chan dataToWrite)
	storageEngine.mapChannel = make(chan dataToMap)
	storageEngine.shutdownTriggerChannel = make(chan struct{})
	storageEngine.shutdownResponseChannel = make(chan int)

	expectedKey := [32]byte{
		0x98, 0x48, 0x3c, 0x6e, 0xb4, 0x0b, 0x6c, 0x31,
		0xa4, 0x48, 0xc2, 0x2a, 0x66, 0xde, 0xd3, 0xb5,
		0xe5, 0xe8, 0xd5, 0x11, 0x9c, 0xac, 0x83, 0x27,
		0xb6, 0x55, 0xc8, 0xb5, 0xc4, 0x83, 0x64, 0x89}

	go (func () {
		for {
			info := <- storageEngine.dataChannel
			shouldEqual(t, info.key, expectedKey[:])
			shouldEqual(t, info.value, []byte("somedata"))
			info.responseChannel <- 0
		}
	})()

	err := storageEngine.Set("testkey", "somedata")

	if (err != nil) {
		t.Errorf("%v did not equal expected nil", err)
	}
}

func Test_StorageEngine_Set_shouldReturnErrorWhenNonZeroOnResponseChannel(t *testing.T) {
	storageEngine := new(StorageEngine)
	storageEngine.dataFile = mockReadEngineFile{}
	storageEngine.mapFile = mockReadEngineFile{}
	storageEngine.dataChannel = make(chan dataToWrite)
	storageEngine.mapChannel = make(chan dataToMap)
	storageEngine.shutdownTriggerChannel = make(chan struct{})
	storageEngine.shutdownResponseChannel = make(chan int)

	go (func () {
		for {
			info := <- storageEngine.dataChannel
			info.responseChannel <- 1
		}
	})()

	err := storageEngine.Set("testkey", "somedata")
	shouldEqual(t, err, errors.New("Error performing Set"))
}