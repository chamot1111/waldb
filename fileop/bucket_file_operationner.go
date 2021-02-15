package fileop

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	"github.com/chamot1111/waldb/config"
	"github.com/chamot1111/waldb/wutils"
	"go.uber.org/zap"
)

const headerSize = 8

type fileData struct {
	file         *os.File
	key          string
	touchElement *list.Element
}

type fileBatchOpWithFile struct {
	op *FileBatchOp
	fd *fileData
}

// BucketFileOperationner will manage the concurrency to access files
type BucketFileOperationner struct {
	config config.Config
	logger *zap.Logger

	fileDataMap map[string]*fileData
	touchOrder  *list.List // values are fileData key
}

// InitBucketFileOperationner init a BucketFileOperationner
func InitBucketFileOperationner(c config.Config, logger *zap.Logger) (*BucketFileOperationner, error) {
	res := &BucketFileOperationner{
		fileDataMap: make(map[string]*fileData),
		touchOrder:  list.New(),
		logger:      logger,
		config:      c,
	}

	return res, nil
}

func (bfo *BucketFileOperationner) cleanFileData(fd *fileData) error {
	f := fd.file
	fd.file = nil
	if f != nil {
		if err := f.Close(); err != nil {
			return err
		}
	}
	fd.touchElement = nil
	return nil
}

func (bfo *BucketFileOperationner) limitOpenFiles() {
	cur := bfo.touchOrder.Back()
	for bfo.touchOrder.Len() > bfo.config.MaxFileOpen && cur != nil {
		lastFileDataKey := cur.Value.(string)
		prev := cur.Prev()
		err := bfo.removeFileData(lastFileDataKey)
		if err != nil {
			bfo.logger.Error("could not remove fileData", zap.String("container-file", lastFileDataKey), zap.Error(err))
		}
		cur = prev
	}
}

func (bfo *BucketFileOperationner) removeFileData(key string) error {
	fd := bfo.fileDataMap[key]
	cur := fd.touchElement
	delete(bfo.fileDataMap, key)
	if err := bfo.cleanFileData(fd); err != nil {
		return err
	}
	bfo.touchOrder.Remove(cur)
	return nil
}

func (bfo *BucketFileOperationner) getFileDataAndTouch(cf config.ContainerFile) (*fileData, error) {
	key := cf.Key()
	fd, exists := bfo.fileDataMap[key]
	if !exists {
		path := cf.PathToFile(bfo.config)
		folder := cf.BaseFolder(bfo.config)
		os.MkdirAll(folder, 0744)
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0744)
		if err != nil {
			return nil, fmt.Errorf("could not open file %s: %w", path, err)
		}

		e := bfo.touchOrder.PushFront(key)
		fd = &fileData{
			touchElement: e,
			key:          key,
			file:         file,
		}
		bfo.fileDataMap[key] = fd
	} else {
		bfo.touchOrder.MoveToFront(fd.touchElement)
	}
	return fd, nil
}

func (bfo *BucketFileOperationner) getFileDataLimitAndTouch(cf config.ContainerFile) (*fileData, error) {
	bfo.limitOpenFiles()
	res, err := bfo.getFileDataAndTouch(cf)
	return res, err
}

// Close close all file and clear all data
func (bfo *BucketFileOperationner) Close() {
	bfo.touchOrder = list.New()
	for _, v := range bfo.fileDataMap {
		err := bfo.cleanFileData(v)
		if err != nil {
			bfo.logger.Error("could not clear fileData during close", zap.Error(err))
		}
	}
	bfo.fileDataMap = make(map[string]*fileData)
}

// WriteAt write to a file
func (bfo *BucketFileOperationner) WriteAt(cf *config.ContainerFile, buf []byte, offset int64, fileSize int64) error {
	fileData, err := bfo.getFileDataLimitAndTouch(*cf)
	if err != nil {
		return err
	}
	return WriteAtomicOp(buf, uint64(offset), uint64(fileSize), fileData.file)
}

// Truncate truncate file
func (bfo *BucketFileOperationner) Truncate(cf *config.ContainerFile, offset int64) error {
	fileData, err := bfo.getFileDataLimitAndTouch(*cf)
	if err != nil {
		return err
	}
	return TruncateAtomicOp(uint64(offset), fileData.file)
}

// Archive file
func (bfo *BucketFileOperationner) Archive(cf *config.ContainerFile, shardIndex, walIndex, operationIndex int, deleteActiveFile bool) (deletedFile bool, err error) {
	fileData, err := bfo.getFileDataLimitAndTouch(*cf)
	if err != nil {
		return false, err
	}

	return ArchiveAtomicOp(fileData.file, cf.PathToFile(bfo.config), cf.ArchivePath(bfo.config.ArchiveFolder, shardIndex, walIndex, operationIndex), deleteActiveFile, false)
}

// ApplyBatchOp execute the operations in one atomic operation
func (bfo *BucketFileOperationner) ApplyBatchOp(files []*FileBatchOp) ErrorFOPList {
	const maxJobCount = 100
	jobCount := len(files)
	if jobCount > maxJobCount {
		jobCount = maxJobCount
	}
	if jobCount > bfo.config.MaxFileOpen {
		jobCount = bfo.config.MaxFileOpen
	}
	if jobCount < 1 {
		jobCount = 1
	}
	errors := ErrorFOPList{}
	wg := &sync.WaitGroup{}
	finishChan := make(chan *fileData, bfo.config.MaxFileOpen)
	errorChan := make(chan ErrorFOP, len(files))
	jobQueue := make(chan fileBatchOpWithFile, jobCount)
	for i := 0; i < jobCount; i++ {
		wg.Add(1)
		go loopOpApplyer(bfo.config, jobQueue, errorChan, finishChan, wg)
	}

	for i := 0; i < bfo.config.MaxFileOpen; i++ {
		finishChan <- nil
	}

	for _, f := range files {
		finishFd := <-finishChan
		if finishFd != nil {
			bfo.removeFileData(finishFd.key)
		}
		fd, err := bfo.getFileDataLimitAndTouch(f.ContainerFile)
		if err != nil {
			errorChan <- ErrorFOP{
				Err:            err,
				OperationIndex: -1,
				Fbo:            f,
			}
			finishChan <- nil
			continue
		}

		jobQueue <- fileBatchOpWithFile{
			op: f,
			fd: fd,
		}
	}

	close(jobQueue)
	wg.Wait()
	close(finishChan)
	for finishFd := range finishChan {
		if finishFd != nil && finishFd.file == nil {
			bfo.removeFileData(finishFd.key)
		}
	}
	close(errorChan)
	for e := range errorChan {
		errors.Add(e)
	}
	return errors
}

// WriteAtomicOp write atomic op
func WriteAtomicOp(buffer []byte, offset uint64, fileSize uint64, file *os.File) error {
	_, err := file.WriteAt(buffer, int64(offset+headerSize))
	if err != nil {
		return err
	}
	var fs [8]byte
	binary.BigEndian.PutUint64(fs[:], fileSize+headerSize)
	_, err = file.WriteAt(fs[:], 0)
	return err
}

// TruncateAtomicOp truncate atomic op
func TruncateAtomicOp(offset uint64, file *os.File) error {
	if err := file.Truncate(int64(offset) + headerSize); err != nil {
		return err
	}
	var fs [8]byte
	binary.BigEndian.PutUint64(fs[:], offset+headerSize)
	_, err := file.WriteAt(fs[:], 0)
	return err
}

// ArchiveAtomicOp archive atomic op
func ArchiveAtomicOp(file *os.File, activePath, archivePath string, deleteActiveFile, deleteInsteadOfArchiving bool) (isActiveFileDeeleted bool, err error) {
	destPath := archivePath
	if !deleteInsteadOfArchiving {
		if _, err := os.Stat(destPath); os.IsNotExist(err) {
			destPathTmp := destPath + ".tmp"
			if err := os.MkdirAll(path.Dir(destPathTmp), 0744); err != nil {
				return false, err
			}
			destFileTmp, err := os.OpenFile(destPathTmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0744)
			if err != nil {
				return false, fmt.Errorf("could not open file for archiving %s: %w", destPath, err)
			}
			_, err = file.Seek(0, 0)
			if err != nil {
				return false, err
			}
			_, err = io.Copy(destFileTmp, file)
			if err != nil {
				return false, err
			}
			err = destFileTmp.Close()
			if err != nil {
				return false, err
			}
			err = wutils.MoveFile(destPathTmp, destPath)
			if err != nil {
				return false, err
			}
		}
	}

	if deleteActiveFile {
		if err := file.Close(); err != nil {
			return false, err
		}
		if err := os.Remove(activePath); err != nil {
			return false, err
		}
		return true, err
	}

	err = file.Truncate(0)
	return false, err
}

func loopOpApplyer(config config.Config, jobQueue chan fileBatchOpWithFile, errorChan chan ErrorFOP, finishChan chan *fileData, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobQueue {
		for iCmd, cmd := range job.op.Ops {
			switch cmd.OpKind {
			case WriteOp:
				cmd.Buffer.ResetRead()
				err := WriteAtomicOp(cmd.Buffer.Bytes(), uint64(cmd.Offset), uint64(cmd.FileSize), job.fd.file)
				if err != nil {
					errorChan <- ErrorFOP{
						Err:            fmt.Errorf("could not write to the file during loop: %w", err),
						OperationIndex: int(cmd.OperationIndex),
						Fbo:            job.op,
					}
					goto Next
				}
			case ArchiveOp:
				fileDeleted, err := ArchiveAtomicOp(job.fd.file, cmd.ActiveFileName, cmd.ArchiveFileName, len(job.op.Ops)-1 == iCmd, config.DeleteInsteadOfArchiving)
				if fileDeleted {
					job.fd.file = nil
				}
				if err != nil {
					errorChan <- ErrorFOP{
						Err:            err,
						OperationIndex: int(cmd.OperationIndex),
						Fbo:            job.op,
					}
					goto Next
				}
			case TruncateOp:
				err := TruncateAtomicOp(uint64(cmd.Offset), job.fd.file)
				if err != nil {
					errorChan <- ErrorFOP{
						Err:            err,
						OperationIndex: int(cmd.OperationIndex),
						Fbo:            job.op,
					}
					goto Next
				}
			}
		}
		if job.fd.file != nil {
			if err := job.fd.file.Sync(); err != nil {
				errorChan <- ErrorFOP{
					Err:            err,
					OperationIndex: -1,
					Fbo:            job.op,
				}
			}
		}
	Next:
		finishChan <- job.fd
	}
}

// Sync file
func (bfo *BucketFileOperationner) Sync(cf *config.ContainerFile) error {
	fileData, err := bfo.getFileDataLimitAndTouch(*cf)
	if err != nil {
		return err
	}

	return fileData.file.Sync()
}

// GetFileBuffer get the file content
func (bfo *BucketFileOperationner) GetFileBuffer(cf *config.ContainerFile, fileBuf *wutils.Buffer) error {
	fileBuf.Reset()
	fileData, err := bfo.getFileDataLimitAndTouch(*cf)
	if err != nil {
		return err
	}

	_, err = fileData.file.Seek(0, 0)
	if err != nil {
		return err
	}
	var fileSizeB [8]byte
	n, err := io.ReadFull(fileData.file, fileSizeB[:])
	if n == 0 && err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	fileSize := binary.BigEndian.Uint64(fileSizeB[:])

	_, err = fileBuf.ReadFrom(fileData.file)
	if err != nil {
		return err
	}

	fileBuf.Truncate(int(fileSize) - 8)
	return err
}

// CurFileSize get cur file size
func (bfo *BucketFileOperationner) CurFileSize(cf *config.ContainerFile) (int64, error) {
	fileData, err := bfo.getFileDataLimitAndTouch(*cf)
	if err != nil {
		return 0, err
	}

	n, err := fileData.file.Seek(0, 2)
	if err != nil {
		return n, err
	}

	if n == 0 {
		return n, nil
	}

	return n - headerSize, nil
}
