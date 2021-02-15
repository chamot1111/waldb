package wal

import (
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

var backgroundReplicatorStarted bool = false

// Replicator use the archive wal file to make a live copy of the files or will archive the wal in another folder or both.
// As it's completely unrelated to the bdd, we can copy file on a slow fs.
type Replicator struct {
	archiveEventChan chan string
	activeFolder     string // if activeFolder is empty: no replication
	archiveFolder    string
	logger           *zap.Logger
	wg               *sync.WaitGroup
	end              bool
	archiveCmd       string // if archiveCmd is empty: no archive cmd is launched
}

// InitReplicator init a replicator
func InitReplicator(archiveEventChans []chan string, activeFolder, archiveFolder string, archiveCmd string, logger *zap.Logger) *Replicator {
	var archiveEventChan chan string = mergeStringChans(archiveEventChans)
	return &Replicator{
		archiveEventChan: archiveEventChan,
		activeFolder:     activeFolder,
		archiveFolder:    archiveFolder,
		logger:           logger,
		wg:               &sync.WaitGroup{},
		archiveCmd:       archiveCmd,
	}
}

// InitReplicatorWithOneFile use when reloading actual wal file
func InitReplicatorWithOneFile(path string, activeFolder, archiveFolder string, archiveCmd string, logger *zap.Logger) *Replicator {
	c := make(chan string, 1)
	c <- path
	return &Replicator{
		archiveEventChan: c,
		activeFolder:     activeFolder,
		archiveFolder:    archiveFolder,
		logger:           logger,
		wg:               &sync.WaitGroup{},
		archiveCmd:       archiveCmd,
	}
}

func (r *Replicator) replicationActivated() bool {
	return r.activeFolder != ""
}

func (r *Replicator) archiveCmdActivated() bool {
	return r.archiveCmd != ""
}

// Execute synchronously the replicator
func (r *Replicator) Execute() error {
	for archiveWalFilePath := range r.archiveEventChan {
		walFile, err := ReadFileFromPath(archiveWalFilePath)
		if err != nil {
			return err
		}
		err = r.coldReplay(archiveWalFilePath, walFile)
		if err != nil {
			return err
		}
	}
	return nil
}

// Start execute the replicator
func (r *Replicator) Start() {
	backgroundReplicatorStarted = true
	r.wg.Add(1)
	go r.loop()
}

// Stop execute the replicator and wait for it to finish current wal
func (r *Replicator) Stop() {
	r.end = true
	r.wg.Wait()
	backgroundReplicatorStarted = false
}

func (r *Replicator) loop() {
	defer r.wg.Done()
	for archiveWalFilePath := range r.archiveEventChan {
		if r.replicationActivated() {
			walFile, err := ReadFileFromPath(archiveWalFilePath)
			if err != nil {
				r.logger.Error("Replicator: could not read wal file", zap.String("archive path", archiveWalFilePath), zap.Error(err))
				return
			}
			for true {
				err := r.coldReplay(archiveWalFilePath, walFile)
				if err == nil {
					break
				}

				r.logger.Info("Replicator: cold replay fail retry in 10 seconds", zap.Error(err))
				time.Sleep(10 * time.Second)
			}
		}
		if r.archiveCmd != "" {
			for true {
				cmdExpanded := strings.ReplaceAll(r.archiveCmd, "%p", archiveWalFilePath)
				cmdExpanded = strings.ReplaceAll(cmdExpanded, "%f", path.Base(archiveWalFilePath))
				cmd := exec.Command("/bin/sh", "-c", cmdExpanded)
				r.logger.Info("Replicator: Running archive command and waiting for it to finish ...", zap.String("cmd", r.archiveCmd))
				err := cmd.Run()
				if err == nil {
					break
				}

				r.logger.Info("Replicator: Running archive command fail. retry in 10 seconds", zap.Error(err))
				time.Sleep(10 * time.Second)
			}
		}
		if r.end {
			r.logger.Info("Replicator: stop due to end")
			return
		}
	}
	r.logger.Info("Replicator: stop due to channel close")
}

func (r *Replicator) coldReplay(archiveWalFilePath string, walFile *File) error {
	errors := walFile.ColdReplay(r.activeFolder, r.archiveFolder)
	if errors.Err() != nil {

		r.logger.Error("Replicator: fail cold replay", zap.String("archive-path", archiveWalFilePath), zap.Error(errors.Err()))
	}

	err := os.Remove(archiveWalFilePath)
	if err != nil {
		r.logger.Info("Replicator: fail delete wal file", zap.String("archive-path", archiveWalFilePath))
		return err
	}

	return nil
}

func mergeStringChans(cs []chan string) chan string {
	out := make(chan string)
	var wg sync.WaitGroup
	wg.Add(len(cs))
	for _, c := range cs {
		go func(c chan string) {
			for v := range c {
				out <- v
			}
			wg.Done()
		}(c)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
