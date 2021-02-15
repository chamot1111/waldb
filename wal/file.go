package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/chamot1111/waldb/config"
	"github.com/chamot1111/waldb/fileop"
	"github.com/chamot1111/waldb/wutils"
)

type cmdKind uint8

const (
	writeCmd    cmdKind = iota
	archiveCmd  cmdKind = iota
	truncateCmd cmdKind = iota
)

const successOperationCount = 40000 * 8

const curWalVersion = 1

type walCmd struct {
	cf             config.ContainerFile
	cmd            cmdKind
	buffer         *wutils.Buffer
	writeOffset    uint64
	fileSize       uint64
	operationIndex uint32
	retryCount     uint8
}

// File content
type File struct {
	cmdsPerFile      map[string][]*walCmd
	cmdsOrder        []*walCmd
	walIndex         uint64
	shardIndex       uint64
	shardCount       uint64
	unixCreationTime uint64
	successOperation []byte
}

func initFile(walIndex, shardIndex, shardCount int) *File {
	return &File{
		cmdsPerFile:      make(map[string][]*walCmd),
		cmdsOrder:        make([]*walCmd, 0),
		successOperation: make([]byte, successOperationCount/8),
		unixCreationTime: uint64(time.Now().Unix()),
		walIndex:         uint64(walIndex),
		shardIndex:       uint64(shardIndex),
		shardCount:       uint64(shardCount),
	}
}

func initFileForRead() *File {
	return &File{
		cmdsPerFile:      make(map[string][]*walCmd),
		cmdsOrder:        make([]*walCmd, 0),
		successOperation: make([]byte, successOperationCount/8),
		unixCreationTime: uint64(time.Now().Unix()),
	}
}

func (wf *File) addCmd(cmd *walCmd) {
	wf.cmdsOrder = append(wf.cmdsOrder, cmd)
	key := cmd.cf.Key()
	if _, exists := wf.cmdsPerFile[key]; !exists {
		wf.cmdsPerFile[key] = make([]*walCmd, 0)
	}
	wf.cmdsPerFile[key] = append(wf.cmdsPerFile[key], cmd)
}

// SetSuccessOperation set success operation value
func (wf *File) setSuccessOperation(operationIndex int, value bool) {
	byteIndex := operationIndex / 8
	bitIndex := operationIndex % 8
	b := wf.successOperation[byteIndex]
	if value {
		b = b | 1<<bitIndex
	} else {
		b = b &^ (1 << bitIndex)
	}
	wf.successOperation[byteIndex] = b
}

// GetSuccessOperation get the success operation status
func (wf *File) getSuccessOperation(operationIndex int) bool {
	byteIndex := operationIndex / 8
	bitIndex := operationIndex % 8
	if byteIndex > len(wf.successOperation) {
		panic("getSuccessOperation: out of bound index")
	}
	b := wf.successOperation[byteIndex]
	return (b & (1 << bitIndex)) != 0
}

func (wf *File) syncSuccessOperation(file *os.File) error {
	const headerCurWalVersionLen = 1
	const headerWalIndexLen = 8
	const headerUnixCreationTimeLen = 8
	const headerShardCountLen = 8
	const headerShardIndexLen = 8
	_, err := file.WriteAt(wf.successOperation, offsetSuccessOperationBytes)
	if err != nil {
		return err
	}
	return file.Sync()
}

func (wf *File) reset() {
	wf.resetWithNewElems(make(map[string][]*walCmd), wf.cmdsOrder[0:0], wf.walIndex)
}

func (wf *File) resetWithNewElems(perFile map[string][]*walCmd, cmdsOrder []*walCmd, walIndex uint64) {
	wf.walIndex = walIndex
	wf.cmdsPerFile = perFile
	wf.cmdsOrder = cmdsOrder
	for i := range wf.successOperation {
		wf.successOperation[i] = 0
	}
}

func (wf *File) writeAllCmdToFile(buffer *bufio.Writer) error {
	for _, cmd := range wf.cmdsOrder {
		_, err := wf.writeCmdToFile(buffer, cmd)
		if err != nil {
			return err
		}
	}
	return nil
}

const headerCurWalVersionLen = 1
const headerWalIndexLen = 8
const headerUnixCreationTimeLen = 8
const headerShardCountLen = 8
const headerShardIndexLen = 8
const offsetSuccessOperationBytes = headerCurWalVersionLen + headerWalIndexLen + headerUnixCreationTimeLen + headerShardCountLen + headerShardIndexLen

func (wf *File) writeHeader(buffer *bufio.Writer) error {
	wf.unixCreationTime = uint64(time.Now().Unix())

	err := buffer.WriteByte(curWalVersion)
	if err != nil {
		return err
	}

	err = writeUint64(buffer, wf.walIndex)
	if err != nil {
		return err
	}

	err = writeUint64(buffer, wf.unixCreationTime)
	if err != nil {
		return err
	}

	err = writeUint64(buffer, wf.shardCount)
	if err != nil {
		return err
	}

	err = writeUint64(buffer, wf.shardIndex)
	if err != nil {
		return err
	}

	_, err = buffer.Write(wf.successOperation)
	return err
}

func (wf *File) readHeader(reader *bufio.Reader) error {
	fileVersion, err := reader.ReadByte() // wal version
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}

	if int(fileVersion) != curWalVersion {
		return fmt.Errorf("try to open a wal file version: %d", curWalVersion)
	}

	walIndex, err := readUint64(reader)
	if err != nil {
		return err
	}
	wf.walIndex = walIndex

	unixCreationTime, err := readUint64(reader)
	if err != nil {
		return err
	}
	wf.unixCreationTime = unixCreationTime

	shardCount, err := readUint64(reader)
	if err != nil {
		return err
	}
	wf.shardCount = shardCount

	shardIndex, err := readUint64(reader)
	if err != nil {
		return err
	}
	wf.shardIndex = shardIndex

	_, err = io.ReadFull(reader, wf.successOperation)

	return err
}

func (wf *File) writeCmdToFile(buffer *bufio.Writer, cmd *walCmd) (int, error) {
	n := 0
	key := cmd.cf.Key()
	if len(key) > 255 {
		return 0, fmt.Errorf("key is more than 255 chars: %s", key)
	}
	if err := buffer.WriteByte(byte(len(key))); err != nil {
		return 0, err
	}
	n = n + 1
	if _, err := buffer.WriteString(key); err != nil {
		return 0, err
	}
	n = n + len(key)
	if err := buffer.WriteByte(byte(cmd.cmd)); err != nil {
		return 0, err
	}
	n = n + 1
	var lenBuffer [8]byte
	if cmd.buffer != nil {
		binary.BigEndian.PutUint64(lenBuffer[:], uint64(cmd.buffer.Len()))
	} else {
		binary.BigEndian.PutUint64(lenBuffer[:], 0)
	}

	if _, err := buffer.Write(lenBuffer[:]); err != nil {
		return 0, err
	}
	n = n + 8

	if cmd.buffer != nil {
		cmd.buffer.ResetRead()
		n = n + cmd.buffer.Len() // must be call before write
		if _, err := buffer.Write(cmd.buffer.Bytes()); err != nil {
			cmd.buffer.ResetRead()
			return 0, err
		}
		cmd.buffer.ResetRead()
	}

	var offset [8]byte
	binary.BigEndian.PutUint64(offset[:], uint64(cmd.writeOffset))
	if _, err := buffer.Write(offset[:]); err != nil {
		return 0, err
	}
	n = n + 8

	var fileSize [8]byte
	binary.BigEndian.PutUint64(fileSize[:], uint64(cmd.fileSize))
	if _, err := buffer.Write(fileSize[:]); err != nil {
		return 0, err
	}
	n = n + 8

	if err := buffer.WriteByte(byte(cmd.retryCount)); err != nil {
		return 0, err
	}
	n = n + 1

	return n, nil
}

// ReadFileFromPath read a wal file from a specific path
func ReadFileFromPath(path string) (*File, error) {
	var err error
	res := initFileForRead()

	file, err := os.Open(path)
	defer file.Close()
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(file)

	err = res.readHeader(reader)
	if err != nil {
		return nil, err
	}
	curIndex := 0
	for true {
		var cmd *walCmd
		cmd, err = readCmdFromReader(reader, curIndex)
		if cmd != nil {
			res.cmdsOrder = append(res.cmdsOrder, cmd)
			key := cmd.cf.Key()
			if _, exist := res.cmdsPerFile[key]; !exist {
				res.cmdsPerFile[key] = make([]*walCmd, 0, 10)
			}
			res.cmdsPerFile[key] = append(res.cmdsPerFile[key], cmd)
			curIndex++
		}
		if err != nil {
			break
		}
	}
	if err != io.EOF {
		return res, err
	}

	return res, nil
}

func readCmdFromReader(reader *bufio.Reader, curIndex int) (*walCmd, error) {

	lenKey, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	var bKeyBuf [255]byte
	keyBuf := bKeyBuf[0:lenKey]
	_, err = reader.Read(keyBuf)
	if err != nil {
		return nil, err
	}

	cf, err := config.ParseContainerFileKey(string(keyBuf))
	if err != nil {
		return nil, err
	}

	cmd, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	var bLenBufferBuf [8]byte
	_, err = reader.Read(bLenBufferBuf[:])
	if err != nil {
		return nil, err
	}
	lenBuffer := binary.BigEndian.Uint64(bLenBufferBuf[:])

	var dataBuffer *wutils.Buffer
	if lenBuffer > 0 {
		dataBuffer = &wutils.Buffer{}
		dataBuffer.GrowAndKeepSpace(int(lenBuffer))
		_, err = reader.Read(dataBuffer.Bytes())
		if err != nil {
			return nil, err
		}
	}

	var bOffset [8]byte
	_, err = reader.Read(bOffset[:])
	if err != nil {
		return nil, err
	}

	offset := binary.BigEndian.Uint64(bOffset[:])

	var bFileSize [8]byte
	_, err = reader.Read(bFileSize[:])
	if err != nil {
		return nil, err
	}

	retryCount, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}

	fileSize := binary.BigEndian.Uint64(bFileSize[:])
	return &walCmd{
		cf:             *cf,
		cmd:            cmdKind(cmd),
		buffer:         dataBuffer,
		writeOffset:    offset,
		fileSize:       fileSize,
		retryCount:     retryCount,
		operationIndex: uint32(curIndex),
	}, nil

}

// ColdReplay will replay all the cmds of the wal file
func (wf *File) ColdReplay(activeFolder, archiveFolder string) wutils.ErrorList {
	errors := wutils.ErrorList{}
	for key, perFile := range wf.cmdsPerFile {
		cf, err := config.ParseContainerFileKey(key)
		if err != nil {
			errors.Add(err)
			break
		}
		filePath := cf.PathToFileFromFolder(activeFolder)
		if err := os.MkdirAll(path.Dir(filePath), 0744); err != nil {
			errors.Add(err)
			return errors
		}
		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0744)
		if err != nil {
			errors.Add(err)
			break
		}
		for iCmd, cmd := range perFile {
			switch cmd.cmd {
			case writeCmd:
				cmd.buffer.ResetRead()
				err = fileop.WriteAtomicOp(cmd.buffer.Bytes(), uint64(cmd.writeOffset), uint64(cmd.fileSize), file)
				if err != nil {
					errors.Add(err)
					break
				}
			case truncateCmd:
				err = fileop.TruncateAtomicOp(uint64(cmd.writeOffset), file)
				if err != nil {
					errors.Add(err)
					break
				}
			case archiveCmd:
				archiveFileName := cf.ArchivePath(archiveFolder, int(wf.shardIndex), int(wf.walIndex), int(cmd.operationIndex))
				deletedFile, err := fileop.ArchiveAtomicOp(file, cf.PathToFileFromFolder(activeFolder), archiveFileName, len(perFile)-1 == iCmd, archiveFolder == "")
				if err != nil {
					errors.Add(err)
					break
				}
				if deletedFile {
					file = nil
				}
			}
		}
	}
	return errors
}

func writeUint64(buffer *bufio.Writer, val uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], val)
	_, err := buffer.Write(b[:])
	return err
}

func readUint64(buffer *bufio.Reader) (uint64, error) {
	var b [8]byte
	_, err := buffer.Read(b[:])
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b[:]), nil
}
