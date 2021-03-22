package wal

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/chamot1111/waldb/config"
	"github.com/chamot1111/waldb/fileop"
	"github.com/chamot1111/waldb/wutils"
	"go.uber.org/zap"
)

type shardWALRessource struct {
	w     *WAL
	mutex *sync.Mutex
}

// ArchivedFileFuncter func called when a new archived file is created
type ArchivedFileFuncter interface {
	Do(path string, file config.ContainerFile)
	Close()
}

// ShardWAL is a shard of wal to maximise cpu bound operation and
// and spread checpoint operations
type ShardWAL struct {
	wals                        []*shardWALRessource
	config                      config.Config
	logger                      *zap.Logger
	currentCheckpointShardIndex int32
	archivedFileFuncter         ArchivedFileFuncter
	backgroundExclusiveTask     *sync.Mutex
	archivedChan                chan string
}

// InitShardWAL init a shard wal
func InitShardWAL(config config.Config, logger *zap.Logger, archivedFileFuncter ArchivedFileFuncter) (*ShardWAL, error) {
	logger.Info("InitShardWAL")
	res := &ShardWAL{
		config:                      config,
		logger:                      logger,
		currentCheckpointShardIndex: -1,
		archivedFileFuncter:         archivedFileFuncter,
		backgroundExclusiveTask:     &sync.Mutex{},
	}

	wals := make([]*shardWALRessource, 0, config.ShardCount)

	for i := 0; i < config.ShardCount; i++ {
		logger.Info("InitBucketFileOperationner", zap.Int("index", i))
		bfo, err := fileop.InitBucketFileOperationner(config, logger)
		if err != nil {
			return nil, err
		}
		logger.Info("InitWAL", zap.Int("index", i))
		wal, err := InitWAL(bfo, config, i, logger, &res.currentCheckpointShardIndex)
		if err != nil {
			return nil, err
		}
		wr := &shardWALRessource{
			w:     wal,
			mutex: &sync.Mutex{},
		}

		wals = append(wals, wr)
	}

	res.wals = wals
	logger.Info("InitShardWAL::getArchiveFileCreatedEventChan")
	res.archivedChan = res.getArchiveFileCreatedEventChan()
	go archivedFileRountine(res.archivedChan, res.archivedFileFuncter, res.backgroundExclusiveTask, logger)
	return res, nil
}

// ExecRsyncCommand will clean up, pause, execute the rsync command and resume
func (swa *ShardWAL) ExecRsyncCommand(params map[string][]string) ([]byte, error) {
	swa.backgroundExclusiveTask.Lock()
	defer swa.backgroundExclusiveTask.Unlock()
	swa.renewArchiveFileCreatedEventChan()
	for range swa.archivedChan {
		// flush actual pending archive file
	}
	defer func() {
		swa.archivedChan = swa.getArchiveFileCreatedEventChan()
		go archivedFileRountine(swa.archivedChan, swa.archivedFileFuncter, swa.backgroundExclusiveTask, swa.logger)
	}()
	if swa.config.RsyncCommand == "" {
		swa.logger.Info("No rsync command")
		return nil, nil
	}
	if backgroundReplicatorStarted {
		swa.logger.Info("Could not launch rsync command while replicator running")
		return nil, fmt.Errorf("Could not launch rsync command while replicator running")
	}
	defer func() {
		for _, w := range swa.wals {
			w.mutex.Unlock()
		}
	}()
	for _, w := range swa.wals {
		w.mutex.Lock()
		w.w.suspend()
		defer func(ww *WAL) {
			ww.resumeWALFileChan()
		}(w.w)
	}

	cmdExpanded := strings.ReplaceAll(swa.config.RsyncCommand, "%act", swa.config.ActiveFolder)
	cmdExpanded = strings.ReplaceAll(cmdExpanded, "%arc", swa.config.ArchiveFolder)
	cmdExpanded = strings.ReplaceAll(cmdExpanded, "%walact", swa.config.WALFolder)
	cmdExpanded = strings.ReplaceAll(cmdExpanded, "%walarc", swa.config.WalArchiveFolder)

	for k, v := range params {
		s := ""
		for ii, vv := range v {
			s += vv
			if ii < len(v)-1 {
				s += ","
			}
		}
		cmdExpanded = strings.ReplaceAll(cmdExpanded, "%"+k, s)
	}

	cmd := exec.Command("/bin/sh", "-c", cmdExpanded)
	swa.logger.Info("Running rsync command and waiting for it to finish ...", zap.String("cmd", swa.config.RsyncCommand))
	out, err := cmd.CombinedOutput()
	if err != nil {
		swa.logger.Info("Finish rsync command with error", zap.Error(err))
		return out, err
	}
	swa.logger.Info("Finish rsync command successfully")
	return out, err
}

// GetWalForShardIndex get wal for shard index
func (swa *ShardWAL) GetWalForShardIndex(shardIndex uint32) *WAL {
	return swa.wals[int(shardIndex)].w
}

// LockShardIndex lock
func (swa *ShardWAL) LockShardIndex(shardIndex uint32) {
	swa.wals[int(shardIndex)].mutex.Lock()
}

// UnlockShardIndex unlock
func (swa *ShardWAL) UnlockShardIndex(shardIndex uint32) {
	swa.wals[int(shardIndex)].mutex.Unlock()
}

// FlushAll wal file. It's a blocking operation.
func (swa *ShardWAL) FlushAll() (errOpsCount int, errors wutils.ErrorList) {
	errors = wutils.ErrorList{}
	for i := range swa.wals {
		errOpsCountL, err := swa.FlushShardIndex(uint32(i))
		errors.Add(err)
		errOpsCount = errOpsCount + errOpsCountL
	}
	return errOpsCount, errors
}

// FlushShardIndex flush shard index. It's a blocking operation.
func (swa *ShardWAL) FlushShardIndex(shardIndex uint32) (errOpsCount int, err error) {
	swr := swa.wals[int(shardIndex)]
	swr.mutex.Lock()
	defer swr.mutex.Unlock()
	return swr.w.Flush()
}

// CloseAll wal file. It's a blocking operation.
func (swa *ShardWAL) CloseAll() wutils.ErrorList {
	errors := wutils.ErrorList{}
	for i := range swa.wals {
		err := swa.CloseShardIndex(uint32(i))
		errors.Add(err)
	}
	return errors
}

// CloseShardIndex Close shard index. It's a blocking operation.
func (swa *ShardWAL) CloseShardIndex(shardIndex uint32) error {
	swr := swa.wals[int(shardIndex)]
	swr.mutex.Lock()
	defer swr.mutex.Unlock()
	return swr.w.Close()
}

// GetArchiveEventChan get all archive envent chan
func (swa *ShardWAL) GetArchiveEventChan() []chan string {
	res := make([]chan string, 0, len(swa.wals))
	for _, v := range swa.wals {
		res = append(res, v.w.GetArchiveEventChan())
	}
	return res
}

func archivedFileRountine(ch chan string, archivedFileFunc ArchivedFileFuncter, mutex *sync.Mutex, logger *zap.Logger) {
	for s := range ch {
		if archivedFileFunc != nil {
			archivedFileRountineWithLock(s, archivedFileFunc, mutex, logger)
		}
	}
	archivedFileFunc.Close()
}

func archivedFileRountineWithLock(p string, archivedFileFunc ArchivedFileFuncter, mutex *sync.Mutex, logger *zap.Logger) {
	mutex.Lock()
	defer mutex.Unlock()
	cf, err := config.ParseContainerFileFromArchivePath(p)
	if err != nil {
		logger.Warn(fmt.Sprintf("file %s is not an archive file: discarded for resume archive routine", p), zap.String("path", p), zap.String("err", err.Error()))
		return
	}
	archivedFileFunc.Do(p, *cf)
}

// getArchiveFileCreatedEventChan when an archive file is created by any wal, the ContainerFile key is send to
// this channel
func (swa *ShardWAL) getArchiveFileCreatedEventChan() chan string {
	res := make([]chan string, 0, len(swa.wals))
	for _, v := range swa.wals {
		res = append(res, v.w.GetArchiveFileCreatedEventChan())
	}
	c := mergeStringChans(res, 1000000)
	swa.logger.Info("InitShardWAL::addExistingArchivedFileToChan start", zap.String("archive-folder", swa.config.ArchiveFolder))
	swa.addExistingArchivedFileToChan(c)
	swa.logger.Info("InitShardWAL::addExistingArchivedFileToChan stop")
	return c
}

// renewArchiveFileCreatedEventChan renew all the channel closing the old ones
func (swa *ShardWAL) renewArchiveFileCreatedEventChan() {
	for _, v := range swa.wals {
		v.w.renewArchiveFileCreatedEventChan()
	}
}

func (swa *ShardWAL) addExistingArchivedFileToChan(c chan string) error {
	err := wutils.WalkFolderUnordered(swa.config.ArchiveFolder, func(path string, info os.FileInfo, err error) error {
		if len(c) < cap(c) {
			if info != nil && !info.IsDir() {
				c <- path
			}
		} else {
			swa.logger.Warn("capacity reached, skip archive file", zap.String("path", path))
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
