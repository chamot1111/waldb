package wal

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/chamot1111/waldb/config"
	"github.com/chamot1111/waldb/fileop"
	"github.com/chamot1111/waldb/wutils"
	"go.uber.org/zap"
)

const walArchiveFilePrefix = "wal-"
const maxRetryCount = 2
const archiveFileCreatedEventLen = 1000000
const walFileArchiveEventLen = 1000000

// WAL is used to make sure, we can restore state after a power failure or software failure
type WAL struct {
	logger                     *zap.Logger
	file                       *os.File
	buffer                     *bufio.Writer
	fileSize                   int
	fileExecutor               *fileop.BucketFileOperationner
	persistentState            *PersistentState
	config                     config.Config
	lastCheckpointingTime      time.Time
	mergeBarrierOperationIndex int

	shardIndex int

	currentCheckpointShardIndex *int32

	walFile                 File
	walFileArchiveEvent     chan string
	archiveFileCreatedEvent chan string
}

// InitWAL init the wal file
func InitWAL(fileExecutor *fileop.BucketFileOperationner, c config.Config, shardIndex int, logger *zap.Logger, currentCheckpointShardIndex *int32) (*WAL, error) {
	if c.WalArchiveFolder != "" {
		if err := os.MkdirAll(c.WalArchiveFolder, 0744); err != nil {
			return nil, fmt.Errorf("could not create wal archive folder: %w", err)
		}
	}
	if err := os.MkdirAll(c.WALFolder, 0744); err != nil {
		return nil, fmt.Errorf("could not create folder for wal file: %w", err)
	}

	logger.Info("InitWAL:InitPersistentFileFromDisk")
	persistentState, err := InitPersistentFileFromDisk(c, shardIndex)
	if err != nil {
		return nil, err
	}

	logger.Info("InitWAL:loadExistingWALFile")
	walFile, err := loadExistingWALFile(getWalPath(c, shardIndex), c, shardIndex, logger)
	if err != nil {
		return nil, err
	}

	walFileArchiveEvent := make(chan string, walFileArchiveEventLen)
	archiveFileCreatedEvent := make(chan string, archiveFileCreatedEventLen)

	if c.WalArchiveFolder != "" {
		logger.Info("InitWAL:AddExistingWALFileToChan")
		AddExistingWALFileToChan(walFileArchiveEvent, c.WalArchiveFolder)
	}

	if walFile == nil {
		persistentState.WalIndex++
		err = persistentState.Save()
		if err != nil {
			return nil, err
		}
		logger.Info("InitWAL:initFile")
		walFile = initFile(int(persistentState.WalIndex), shardIndex, c.ShardCount)
	}

	resWal := &WAL{
		logger:                      logger,
		fileExecutor:                fileExecutor,
		walFile:                     *walFile,
		config:                      c,
		persistentState:             persistentState,
		walFileArchiveEvent:         make(chan string, walFileArchiveEventLen),
		lastCheckpointingTime:       time.Now(),
		shardIndex:                  shardIndex,
		mergeBarrierOperationIndex:  -1,
		currentCheckpointShardIndex: currentCheckpointShardIndex,
		archiveFileCreatedEvent:     archiveFileCreatedEvent,
	}

	if len(walFile.cmdsOrder) > 0 {
		n, err := resWal.checkPointing()
		if n > 0 {
			return resWal, fmt.Errorf("load existing wal file has encountered several errors: %d", n)
		}
		if err != nil {
			return resWal, err
		}
	}

	return resWal, err
}

// GetArchiveEventChan for this wal
func (w *WAL) GetArchiveEventChan() chan string {
	return w.walFileArchiveEvent
}

// GetArchiveFileCreatedEventChan when an archive file is created the ContainerFile key is send to
// this channel
func (w *WAL) GetArchiveFileCreatedEventChan() chan string {
	return w.archiveFileCreatedEvent
}

func (w *WAL) renewArchiveFileCreatedEventChan() {
	close(w.archiveFileCreatedEvent)
	w.archiveFileCreatedEvent = make(chan string, archiveFileCreatedEventLen)
}

func loadExistingWALFile(walFilePath string, config config.Config, shardIndex int, logger *zap.Logger) (*File, error) {
	if _, err := os.Stat(walFilePath); os.IsNotExist(err) {
		return nil, nil
	}

	walFile, err := ReadFileFromPath(walFilePath)
	if err != nil {
		if err != ErrBadWalFileCrcCommand {
			return nil, err
		}
		logger.Warn("load existing wal file. Some commands are corrupted", zap.Error(err), zap.String("wal-path", walFilePath))
	}
	if walFile.shardCount != uint64(config.ShardCount) {
		return nil, fmt.Errorf("try to flush wal with a different shard count")
	}
	if walFile.shardIndex != uint64(shardIndex) {
		return nil, fmt.Errorf("try to flush wal with a different shard index")
	}
	return walFile, nil
}

// AddExistingWALFileToChan add wal file in order to this channel
func AddExistingWALFileToChan(c chan string, archiveWalFolder string) error {
	files, err := ioutil.ReadDir(archiveWalFolder)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() || !strings.HasPrefix(file.Name(), walArchiveFilePrefix) {
			continue
		}
		p := path.Join(archiveWalFolder, file.Name())
		c <- p
	}
	return nil
}

// AppendWrite append write to a file
func (w *WAL) AppendWrite(cf config.ContainerFile, buffers ...[]byte) error {
	offset, err := w.curFileSize(cf)
	if err != nil {
		return err
	}
	s := 0
	for _, b := range buffers {
		s = s + len(b)
	}
	return w.Write(cf, offset, offset+int64(s), buffers...)
}

func (w *WAL) curFileSize(cf config.ContainerFile) (int64, error) {
	key := cf.Key()
	cmds := w.walFile.cmdsPerFile[key]

	var offset int64 = 0
	var lastCmd *walCmd
	if cmds != nil && len(cmds) > 0 {
		lastCmd = cmds[len(cmds)-1]
	}
	if lastCmd != nil {
		lastCmd := cmds[len(cmds)-1]
		switch lastCmd.cmd {
		case writeCmd:
			offset = int64(lastCmd.fileSize)
		case archiveCmd:
			offset = 0
		case truncateCmd:
			offset = int64(lastCmd.writeOffset)
		}
	} else {
		var err error
		offset, err = w.fileExecutor.CurFileSize(&cf)
		if err != nil {
			return offset, err
		}
	}

	return offset, nil
}

// Truncate file
func (w *WAL) Truncate(cf config.ContainerFile, offset int64) error {
	if err := w.checkpointIfNecessary(); err != nil {
		return err
	}

	fs, err := w.curFileSize(cf)
	if err != nil {
		return err
	}
	if offset > fs {
		return fmt.Errorf("could not truncate a file of size %d at %d", fs, offset)
	}
	newCmd := &walCmd{
		cf:             cf,
		cmd:            truncateCmd,
		buffer:         nil,
		writeOffset:    uint64(offset),
		operationIndex: uint32(len(w.walFile.cmdsOrder)),
	}
	if w.file == nil {
		if err := w.createNewFile(); err != nil {
			return fmt.Errorf("could not write wal file: %w", err)
		}
	}
	w.walFile.addCmd(newCmd)
	return nil
}

// Archive file
func (w *WAL) Archive(cf config.ContainerFile) error {
	if err := w.checkpointIfNecessary(); err != nil {
		return err
	}

	newCmd := &walCmd{
		cf:             cf,
		cmd:            archiveCmd,
		buffer:         nil,
		operationIndex: uint32(len(w.walFile.cmdsOrder)),
	}
	if w.file == nil {
		if err := w.createNewFile(); err != nil {
			return fmt.Errorf("could not write wal file: %w", err)
		}
	}
	w.walFile.addCmd(newCmd)
	return nil
}

// GetFileBuffer for file
func (w *WAL) GetFileBuffer(cf config.ContainerFile, fileBuf *wutils.Buffer) error {
	fileBuf.Reset()
	err := w.fileExecutor.GetFileBuffer(&cf, fileBuf)
	if err != nil {
		return err
	}
	err = w.updateReadBuffer(cf, fileBuf)

	return err
}

func (w *WAL) Write(cf config.ContainerFile, fileOffset int64, fileSize int64, buffers ...[]byte) error {
	if err := w.checkpointIfNecessary(); err != nil {
		return err
	}

	offset := fileOffset

	lastCmd := w.lastCmdForKey(cf.Key())
	if lastCmd != nil && lastCmd.cmd == writeCmd && int(lastCmd.fileSize) == int(offset) && int(lastCmd.operationIndex) > w.mergeBarrierOperationIndex {
		for _, b := range buffers {
			_, err := lastCmd.buffer.Write(b)
			if err != nil {
				return err
			}
			w.fileSize = w.fileSize + len(b)
		}
		lastCmd.fileSize = uint64(fileSize)
	} else {
		buffer := &wutils.Buffer{}

		endOffset := int(offset)

		for _, b := range buffers {
			endOffset = endOffset + len(b)
			_, err := buffer.Write(b)
			if err != nil {
				return err
			}
		}

		if endOffset > int(fileSize) {
			return fmt.Errorf("file size is not big enough: %d >%d", endOffset, fileSize)
		}

		newWriteCmd := &walCmd{
			cf:             cf,
			cmd:            writeCmd,
			buffer:         buffer,
			writeOffset:    uint64(offset),
			fileSize:       uint64(fileSize),
			operationIndex: uint32(len(w.walFile.cmdsOrder)),
		}

		w.fileSize = w.fileSize + cf.DataSize()
		w.fileSize = w.fileSize + buffer.FullLen()

		if w.file == nil {
			if err := w.createNewFile(); err != nil {
				return fmt.Errorf("could not write wal file: %w", err)
			}
		}

		w.walFile.addCmd(newWriteCmd)
	}

	return nil
}

func (w *WAL) lastCmdForKey(key string) *walCmd {
	cmds := w.walFile.cmdsPerFile[key]
	if cmds == nil || len(cmds) == 0 {
		return nil
	}
	return cmds[len(cmds)-1]
}

func (w *WAL) updateReadBuffer(cf config.ContainerFile, buffer *wutils.Buffer) error {

	key := cf.Key()
	var cmds []*walCmd = w.walFile.cmdsPerFile[key]

	if cmds == nil {
		return nil
	}

	startCmd := 0

	for i := len(cmds) - 1; i >= 0; i-- {
		c := cmds[i]
		if c.cmd == archiveCmd {
			startCmd = i + 1
			buffer.Reset()
			break
		}
	}

	for i := startCmd; i < len(cmds); i++ {
		c := cmds[i]
		switch c.cmd {
		case truncateCmd:
			buffer.Truncate(int(c.writeOffset))
		case writeCmd:
			buffer.Write(c.buffer.Bytes())
		}
	}
	return nil
}

// Flush current wal file
func (w *WAL) Flush() (errOpsCount int, err error) {
	return w.checkPointing()
}

// Close flush current wal file and close all files
func (w *WAL) Close() error {
	_, err := w.Flush()
	if err != nil {
		return err
	}
	w.fileExecutor.Close()
	close(w.walFileArchiveEvent)
	return nil
}

func (w *WAL) suspend() {
	w.Close()
}

func (w *WAL) resumeWALFileChan() {
	w.walFileArchiveEvent = make(chan string, walFileArchiveEventLen)
	if w.config.WalArchiveFolder != "" {
		AddExistingWALFileToChan(w.walFileArchiveEvent, w.config.WalArchiveFolder)
	}
}

func (w *WAL) createNewFile() error {
	var err error
	w.fileSize = 0

	w.file, err = os.OpenFile(getWalPath(w.config, w.shardIndex), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0744)
	if err != nil {
		w.buffer = nil
		return err
	}
	if w.buffer != nil {
		w.buffer.Reset(w.file)
	} else {
		w.buffer = bufio.NewWriter(w.file)
	}
	err = w.walFile.writeHeader(w.buffer)
	return err
}

func getWalPath(c config.Config, shardIndex int) string {
	return path.Join(c.WALFolder, fmt.Sprintf("wal-%05d.bin", shardIndex))
}

func (w *WAL) checkpointIfNecessary() error {
	doCheckpoint := false
	if w.needCheckpointingHardLimit() {
		atomic.CompareAndSwapInt32(w.currentCheckpointShardIndex, -1, int32(w.shardIndex))
		doCheckpoint = true
		w.logger.Info("checkpoint hard limit", zap.Int("shard-index", w.shardIndex))
	} else {
		if w.needCheckpointingSoftLimit() {
			if w.currentCheckpointShardIndex == nil {
				doCheckpoint = true
			} else {
				ownedCheckpoint := atomic.CompareAndSwapInt32(w.currentCheckpointShardIndex, -1, int32(w.shardIndex))
				if ownedCheckpoint {
					doCheckpoint = true
				}
			}
		}
	}

	if doCheckpoint {
		_, err := w.checkPointing()
		return err
	}
	return nil
}

func (w *WAL) needCheckpointingSoftLimit() bool {
	return w.fileSize > w.config.MaxWALFileSize || time.Since(w.lastCheckpointingTime).Seconds() > float64(w.config.MaxWALFileDurationS)
}

func (w *WAL) needCheckpointingHardLimit() bool {
	return len(w.walFile.cmdsOrder) >= successOperationCount
}

func (w *WAL) applying() (errOpsCount int, err error) {
	errOpsCount = 0
	opsPerFile := w.convertUnsucessfulWalCmdsToOps()
	errors := w.fileExecutor.ApplyBatchOp(opsPerFile)
	for _, fop := range opsPerFile {
		var lastSuccessOperationIndex int64 = int64(fop.Ops[len(fop.Ops)-1].OperationIndex)
		errFop := errors.GetErrorForFop(fop)
		if errFop != nil {
			errOpsCount++
			w.logger.Error("wal write opreation", zap.Error(errFop.Err))
			lastSuccessOperationIndex = int64(errFop.OperationIndex)
		}
		if lastSuccessOperationIndex >= 0 {
			for _, op := range fop.Ops {
				w.walFile.setSuccessOperation(int(op.OperationIndex), true)
				if lastSuccessOperationIndex == int64(op.OperationIndex) {
					break
				}
			}
		}
	}

	w.mergeBarrierOperationIndex = int(w.walFile.cmdsOrder[len(w.walFile.cmdsOrder)-1].operationIndex)
	return
}

func (w *WAL) checkPointing() (errOpsCount int, err error) {
	defer func() {
		if w.currentCheckpointShardIndex != nil {
			atomic.CompareAndSwapInt32(w.currentCheckpointShardIndex, int32(w.shardIndex), -1)
		}
	}()
	if w.file == nil {
		return 0, nil
	}
	for _, cmd := range w.walFile.cmdsOrder {
		_, err := w.walFile.writeCmdToFile(w.buffer, cmd)
		if err != nil {
			return 0, err
		}
	}

	if err := w.buffer.Flush(); err != nil {
		return 0, err
	}

	if err := w.file.Sync(); err != nil {
		return 0, err
	}

	if errOpsCount, err = w.applying(); err != nil {
		return errOpsCount, err
	}

	err = w.walFile.syncSuccessOperation(w.file)
	if err != nil {
		return errOpsCount, err
	}

	if err := w.file.Close(); err != nil {
		return errOpsCount, err
	}

	for cfStr, opForFile := range w.walFile.cmdsPerFile {
		for _, op := range opForFile {
			if op.cmd == archiveCmd {
				cf, err := config.ParseContainerFileKey(cfStr)
				if err != nil {
					w.logger.Error("could not parse container file key to send archive file", zap.String("key", cfStr), zap.Error(err))
					continue
				}
				p := cf.ArchivePath(w.config.ArchiveFolder, w.shardIndex, int(w.walFile.walIndex), int(op.operationIndex))
				w.archiveFileCreatedEvent <- p
			}
		}
	}

	w.file = nil

	w.persistentState.WalIndex++
	if err := w.persistentState.Save(); err != nil {
		return errOpsCount, err
	}

	newCmdsPerFile, newCmdsOrder := w.prepareFailedOperationsForNextWal()
	w.walFile.resetWithNewElems(newCmdsPerFile, newCmdsOrder, w.persistentState.WalIndex)

	w.fileSize = 0
	w.mergeBarrierOperationIndex = -1

	archiveFileName := fmt.Sprintf(walArchiveFilePrefix+"%012d-s%05d.bin", w.persistentState.WalIndex, w.shardIndex)
	curWalPath := getWalPath(w.config, w.shardIndex)
	if w.config.WalArchiveFolder != "" {
		fullPathArchive := path.Join(w.config.WalArchiveFolder, archiveFileName)
		err = wutils.MoveFile(curWalPath, fullPathArchive)
		if err != nil {
			return errOpsCount, err
		}
		w.walFileArchiveEvent <- fullPathArchive
	} else {
		err = os.Remove(curWalPath)
		if err != nil {
			return errOpsCount, fmt.Errorf("failed removing wal file %s: %w", curWalPath, err)
		}
	}

	w.lastCheckpointingTime = time.Now()

	return errOpsCount, nil
}

func (w *WAL) prepareFailedOperationsForNextWal() (map[string][]*walCmd, []*walCmd) {
	perFile := make(map[string][]*walCmd, 0)
	linear := make([]*walCmd, 0)

	var newOperationIndex uint32 = 0

	for _, v := range w.walFile.cmdsPerFile {
		var cf config.ContainerFile
		cf = v[0].cf

		fileCmds := make([]*walCmd, 0)

		for _, cmd := range v {
			if w.walFile.getSuccessOperation(int(cmd.operationIndex)) {
				continue
			}
			if cmd.retryCount >= maxRetryCount {
				continue
			}
			newCmd := &walCmd{
				cf:             cf,
				cmd:            cmd.cmd,
				buffer:         cmd.buffer,
				writeOffset:    cmd.writeOffset,
				fileSize:       cmd.fileSize,
				operationIndex: newOperationIndex,
				retryCount:     cmd.retryCount + 1,
			}
			newOperationIndex++
			fileCmds = append(fileCmds, newCmd)
			linear = append(linear, newCmd)
		}
		if len(fileCmds) > 0 {
			perFile[cf.Key()] = fileCmds
		}
	}

	return perFile, linear
}

func (w *WAL) convertUnsucessfulWalCmdsToOps() []*fileop.FileBatchOp {
	res := make([]*fileop.FileBatchOp, 0, len(w.walFile.cmdsPerFile))
	for _, v := range w.walFile.cmdsPerFile {
		var cf config.ContainerFile
		cf = v[0].cf
		fop := &fileop.FileBatchOp{
			ContainerFile: cf,
			Ops:           make([]fileop.Op, 0, len(v)),
		}

		for _, cmd := range v {
			if w.walFile.getSuccessOperation(int(cmd.operationIndex)) {
				continue
			}
			op := fileop.Op{
				Buffer:         cmd.buffer,
				Offset:         cmd.writeOffset,
				OperationIndex: uint64(cmd.operationIndex),
				ActiveFileName: cf.PathToFile(w.config),
			}
			switch cmd.cmd {
			case writeCmd:
				op.OpKind = fileop.WriteOp
				op.FileSize = int64(cmd.fileSize)
			case archiveCmd:
				op.OpKind = fileop.ArchiveOp
				if w.config.ArchiveFolder != "" {
					op.ArchiveFileName = cf.ArchivePath(w.config.ArchiveFolder, w.shardIndex, int(w.walFile.walIndex), int(cmd.operationIndex))
				}
			case truncateCmd:
				op.OpKind = fileop.TruncateOp
			}
			fop.Ops = append(fop.Ops, op)
		}
		if len(fop.Ops) > 0 {
			res = append(res, fop)
		}
	}
	return res
}
