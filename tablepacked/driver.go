package tablepacked

import (
	"sync"

	"github.com/chamot1111/waldb/config"
	"github.com/chamot1111/waldb/wal"
	"github.com/chamot1111/waldb/wutils"
	"go.uber.org/zap"
)

// Driver is the entry point to serve file with the table packed file format
type Driver struct {
	logger      *zap.Logger
	conf        config.Config
	rowDataPool *sync.Pool
	bufferPool  *sync.Pool

	shardWal *wal.ShardWAL
}

// InitDriver init packed table dirver
func InitDriver(conf config.Config, logger *zap.Logger) (*Driver, error) {
	shardWal, err := wal.InitShardWAL(conf, logger)
	if err != nil {
		return nil, err
	}
	return &Driver{
		conf:     conf,
		logger:   logger,
		shardWal: shardWal,
		rowDataPool: &sync.Pool{
			New: func() interface{} {
				return &RowData{
					Data: make([]ColumnData, 0, 10),
				}
			},
		},
		bufferPool: &sync.Pool{
			New: func() interface{} {
				return new(wutils.Buffer)
			},
		},
	}, nil
}

// GetReplicator get replicator
func (d *Driver) GetReplicator() *wal.Replicator {
	return wal.InitReplicator(d.shardWal.GetArchiveEventChan(), d.conf.ReplicationActiveFolder, d.conf.ReplicationArchiveFolder, "", d.logger)
}

// AppendRowData append rows to a container file
func (d *Driver) AppendRowData(cf config.ContainerFile, rows []*RowData) error {
	si := cf.ShardIndex(uint32(d.conf.ShardCount))
	d.shardWal.LockShardIndex(si)
	defer d.shardWal.UnlockShardIndex(si)

	wal := d.shardWal.GetWalForShardIndex(si)

	return appendRowDataToFile(cf, wal, rows)
}

// ReadAllRowData from file
func (d *Driver) ReadAllRowData(cf config.ContainerFile) (TableDataSlice, error) {
	si := cf.ShardIndex(uint32(d.conf.ShardCount))
	d.shardWal.LockShardIndex(si)
	defer d.shardWal.UnlockShardIndex(si)

	wal := d.shardWal.GetWalForShardIndex(si)

	t, err := ReadAllRowDataFromFileCorruptSafe(cf, wal, d.rowDataPool, d.bufferPool)
	if err != nil {
		return TableDataSlice{}, err
	}
	return InitTableDataSlice(t), nil
}

// Archive archive the file
func (d *Driver) Archive(cf config.ContainerFile) error {
	si := cf.ShardIndex(uint32(d.conf.ShardCount))
	d.shardWal.LockShardIndex(si)
	defer d.shardWal.UnlockShardIndex(si)

	wal := d.shardWal.GetWalForShardIndex(si)

	return wal.Archive(cf)
}

// FreeTable free table
func (d *Driver) FreeTable(t TableDataSlice) {
	if t.table.refc == 0 {
		for _, r := range t.table.Data {
			r.Data = r.Data[0:0]
			d.rowDataPool.Put(r)
		}
	}
}

// Flush all pending action to file
func (d *Driver) Flush() (errOpsCount int, err error) {
	errOpsCountL, errors := d.shardWal.FlushAll()
	return errOpsCountL, errors.Err()
}

// Close flush all pending action to file and close all files
func (d *Driver) Close() error {
	return d.shardWal.CloseAll().Err()
}

// ExecRsyncCommand will clean up, pause, execute the rsync command and resume
func (d *Driver) ExecRsyncCommand() ([]byte, error) {
	return d.shardWal.ExecRsyncCommand()
}
