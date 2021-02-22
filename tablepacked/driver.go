package tablepacked

import (
	"database/sql"
	"os"
	"path"
	"sync"

	"github.com/chamot1111/waldb/config"
	"github.com/chamot1111/waldb/fileop"
	"github.com/chamot1111/waldb/wal"
	"github.com/chamot1111/waldb/wutils"
	"go.uber.org/zap"

	// db driver
	_ "github.com/mattn/go-sqlite3"
)

type sqlite3Archiver struct {
	bdByTable           map[string]*sql.DB
	config              config.Config
	logger              *zap.Logger
	tableDescriptorRepo map[string]Table
	rowDataPool         *sync.Pool
}

// Driver is the entry point to serve file with the table packed file format
type Driver struct {
	logger      *zap.Logger
	conf        config.Config
	rowDataPool *sync.Pool
	bufferPool  *sync.Pool

	shardWal            *wal.ShardWAL
	archivedFileFuncter wal.ArchivedFileFuncter
}

func (sa *sqlite3Archiver) Do(p string, file config.ContainerFile) {
	var descriptor Table
	var exist bool
	descriptor, exist = sa.tableDescriptorRepo[file.TableName]
	if !exist {
		sa.logger.Error("could not found table descriptor", zap.String("table", file.TableName))
		return
	}
	db, exist := sa.bdByTable[file.TableName]
	if !exist {
		fdb := path.Join(sa.config.SqliteFolder, file.TableName+".db")
		shouldCreateTable := false
		if _, err := os.Stat(fdb); os.IsNotExist(err) {
			shouldCreateTable = true
		}
		var err error
		db, err = sql.Open("sqlite3", fdb)
		if err != nil {
			sa.logger.Error("could not open sqlite file", zap.String("file", fdb), zap.Error(err))
			return
		}

		if shouldCreateTable {
			sql := "CREATE TABLE " + file.TableName + "("
			for ic, c := range descriptor.Columns {
				sql += c.Name
				switch c.Type {
				case Tuint | Tenum:
					sql += " INTEGER"
				case Tstring:
					sql += " TEXT"
				default:
					sa.logger.Error("unkown type", zap.Int("type", int(c.Type)))
					return
				}
				if ic < len(descriptor.Columns)-1 {
					sql += ", "
				}
			}
			sql += ");"
			if _, err := db.Exec(sql); err != nil {
				sa.logger.Error("could not create sqlite table", zap.String("sql", sql), zap.Error(err))
				return
			}
		}

		sa.bdByTable[file.TableName] = db
	}

	f, err := os.Open(p)
	if err != nil {
		sa.logger.Error("could not open file", zap.String("path", p), zap.Error(err))
		return
	}
	buffer := &wutils.Buffer{}
	err = fileop.GetFileBufferFromFile(f, buffer)
	if err != nil {
		sa.logger.Error("could not get file buffer from file", zap.String("path", p), zap.Error(err))
		return
	}
	buffer.ResetRead()

	err = f.Close()
	if err != nil {
		sa.logger.Error("could not close file", zap.String("path", p), zap.Error(err))
		return
	}

	tableData, err := ReadAllRowDataFromFileBuffer(buffer, sa.rowDataPool)
	if err != nil {
		if _, ok := err.(*ErrBadEndingCRC); !ok {
			sa.logger.Error("could not parse file", zap.String("path", p), zap.Error(err))
		} else {
			sa.logger.Warn("crc error happened on file", zap.String("path", p))
		}
	}

	sql := "INSERT INTO " + file.TableName + "("
	for ic, c := range descriptor.Columns {
		sql += c.Name
		if ic < len(descriptor.Columns)-1 {
			sql += ", "
		}
	}
	values := make([]interface{}, 0)
	sql += ") VALUES ("
	for _, rData := range tableData.Data {
		for ic, c := range descriptor.Columns {
			if ic < len(rData.Data) {
				switch c.Type {
				case Tuint | Tenum:
					values = append(values, rData.Data[ic].EncodedRawValue)
				case Tstring:
					values = append(values, string(rData.Data[ic].Buffer))
				default:
					sa.logger.Error("unkown type", zap.Int("type", int(c.Type)))
					return
				}
			} else {
				sql += "null"
			}
			if ic < len(descriptor.Columns)-1 {
				sql += ", "
			}
		}
	}
	_, err = db.Exec(sql, values...)
	if err != nil {
		sa.logger.Error("could not insert data", zap.String("sql", sql), zap.Error(err))
	}
}

// InitDriver init packed table dirver
func InitDriver(conf config.Config, logger *zap.Logger) (*Driver, error) {
	sqlite3Archiver := &sqlite3Archiver{}
	shardWal, err := wal.InitShardWAL(conf, logger, sqlite3Archiver)
	if err != nil {
		return nil, err
	}
	return &Driver{
		conf:                conf,
		logger:              logger,
		shardWal:            shardWal,
		rowDataPool:         NewRowDataPool(),
		bufferPool:          NewBufPool(),
		archivedFileFuncter: sqlite3Archiver,
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
func (d *Driver) ExecRsyncCommand(params map[string][]string) ([]byte, error) {
	return d.shardWal.ExecRsyncCommand(params)
}
