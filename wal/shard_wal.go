package wal

import (
	"fmt"
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

// ShardWAL is a shard of wal to maximise cpu bound operation and
// and spread checpoint operations
type ShardWAL struct {
	wals   []*shardWALRessource
	config config.Config
	logger *zap.Logger
}

// InitShardWAL init a shard wal
func InitShardWAL(config config.Config, logger *zap.Logger) (*ShardWAL, error) {
	wals := make([]*shardWALRessource, 0, config.ShardCount)

	for i := 0; i < config.ShardCount; i++ {
		bfo, err := fileop.InitBucketFileOperationner(config, logger)
		if err != nil {
			return nil, err
		}
		wal, err := InitWAL(bfo, config, i, logger)
		if err != nil {
			return nil, err
		}
		wr := &shardWALRessource{
			w:     wal,
			mutex: &sync.Mutex{},
		}

		wals = append(wals, wr)
	}

	return &ShardWAL{
		wals:   wals,
		config: config,
		logger: logger,
	}, nil
}

// ExecRsyncCommand will clean up, pause, execute the rsync command and resume
func (swa *ShardWAL) ExecRsyncCommand() error {
	if swa.config.RsyncCommand == "" {
		swa.logger.Info("No rsync command")
		return nil
	}
	if backgroundReplicatorStarted {
		swa.logger.Info("Could not launch rsync command while replicator running")
		return fmt.Errorf("Could not launch rsync command while replicator running")
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
			ww.resume()
		}(w.w)
	}

	cmdExpanded := strings.ReplaceAll(swa.config.RsyncCommand, "%act", swa.config.ActiveFolder)
	cmdExpanded = strings.ReplaceAll(cmdExpanded, "%arc", swa.config.ArchiveFolder)
	cmdExpanded = strings.ReplaceAll(cmdExpanded, "%walact", swa.config.WALFolder)
	cmdExpanded = strings.ReplaceAll(cmdExpanded, "%walarc", swa.config.WalArchiveFolder)

	cmd := exec.Command("/bin/sh", "-c", cmdExpanded)
	swa.logger.Info("Running rsync command and waiting for it to finish ...", zap.String("cmd", swa.config.RsyncCommand))
	err := cmd.Run()
	if err != nil {
		swa.logger.Info("Finish rsync command with error", zap.Error(err))
		return err
	}
	swa.logger.Info("Finish rsync command successfully")
	return err
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
