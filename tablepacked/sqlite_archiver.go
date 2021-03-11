package tablepacked

import (
	"database/sql"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/chamot1111/waldb/config"
	"github.com/chamot1111/waldb/fileop"
	"github.com/chamot1111/waldb/wutils"
	"go.uber.org/zap"
)

type sqlite3Archiver struct {
	bdByTable           map[string]*sql.DB
	config              config.Config
	logger              *zap.Logger
	tableDescriptorRepo map[string]Table
	rowDataPool         *sync.Pool
}

// var instead of const for testing purpose
var maxSqliteRowPerFile = 100000000

// SqliteDbPathForTableName return the path to the sqlite folder
func SqliteDbPathForTableName(conf config.Config, tableName string) string {
	return path.Join(conf.SqliteFolder, tableName+".db")
}

func (sa *sqlite3Archiver) openDbAndRegisterIt(fdb string, file config.ContainerFile, descriptor Table, shouldCreateTable bool) *sql.DB {
	var db *sql.DB
	var err error
	err = os.MkdirAll(path.Dir(fdb), 0744)
	if err != nil {
		sa.logger.Error("could not create folder for sqlite file", zap.String("file", fdb), zap.Error(err))
		return nil
	}
	db, err = sql.Open("sqlite3", fdb)
	if err != nil {
		sa.logger.Error("could not open sqlite file", zap.String("file", fdb), zap.Error(err))
		return nil
	}

	_, err = db.Exec("pragma journal_mode = WAL;")
	if err != nil {
		sa.logger.Error("could not set journal_mode = WAL", zap.String("file", fdb), zap.Error(err))
		return nil
	}
	_, err = db.Exec("pragma synchronous = normal;")
	if err != nil {
		sa.logger.Error("could not set synchronous = normal", zap.String("file", fdb), zap.Error(err))
		return nil
	}

	if shouldCreateTable {
		sql := "CREATE TABLE " + file.TableName + "("
		for ic, c := range descriptor.Columns {
			sql += c.Name
			switch c.Type {
			case Tuint, Tenum:
				sql += " INTEGER"
			case Tstring:
				sql += " TEXT"
			default:
				sa.logger.Error("unkown type", zap.Int("type", int(c.Type)))
				return nil
			}
			if ic < len(descriptor.Columns)-1 {
				sql += ", "
			}
		}
		sql += ");"
		if _, err := db.Exec(sql); err != nil {
			sa.logger.Error("could not create sqlite table", zap.String("sql", sql), zap.Error(err))
			return nil
		}
	}

	sa.bdByTable[file.TableName] = db

	return db
}

func (sa *sqlite3Archiver) Close() {
	for key, db := range sa.bdByTable {
		err := db.Close()
		sa.logger.Error("could not close sqlite table", zap.String("table", key), zap.Error(err))
	}
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
	fdb := SqliteDbPathForTableName(sa.config, file.TableName)
	if !exist {
		shouldCreateTable := false
		if _, err := os.Stat(fdb); os.IsNotExist(err) {
			shouldCreateTable = true
		}
		db = sa.openDbAndRegisterIt(fdb, file, descriptor, shouldCreateTable)
		if db == nil {
			return
		}
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

	maxRowIDSQL := "SELECT rowid FROM " + file.TableName + " ORDER BY rowid DESC LIMIT 1"
	maxRowIDRow := db.QueryRow(maxRowIDSQL)
	var maxRowID int = 0
	err = maxRowIDRow.Scan(&maxRowID)
	if err != nil {
		if err != sql.ErrNoRows {
			sa.logger.Error("could not get max row id", zap.String("sql", maxRowIDSQL), zap.Error(err))
			return
		}
	}

	if maxRowID >= maxSqliteRowPerFile {
		delete(sa.bdByTable, file.TableName)
		sa.logger.Info("archive sqlite file because limit row has been reached", zap.Int("limit", maxSqliteRowPerFile), zap.Int("max-row-id", maxRowID))
		err = db.Close()
		if err != nil {
			sa.logger.Error("could not close db", zap.String("table-name", file.TableName))
			return
		}
		newPath := fmt.Sprintf("%s-%d.bak", fdb, time.Now().Unix())
		err = os.Rename(fdb, newPath)
		if err != nil {
			sa.logger.Error("could not rename db", zap.String("from", fdb), zap.String("to", newPath))
			return
		}

		db = sa.openDbAndRegisterIt(fdb, file, descriptor, true)
		if db == nil {
			return
		}
	}

	if len(tableData.Data) > 0 {
		const batchInsertRow = 100
		curRow := 0
		for curRow < len(tableData.Data) {
			thisRowsMax := curRow + batchInsertRow
			if thisRowsMax >= len(tableData.Data) {
				thisRowsMax = len(tableData.Data)
			}
			thisRows := tableData.Data[curRow:thisRowsMax]
			if len(thisRows) == 0 {
				break
			}
			values := make([]interface{}, 0)
			sql := "INSERT INTO " + file.TableName + "("
			for ic, c := range descriptor.Columns {
				sql += c.Name
				if ic < len(descriptor.Columns)-1 {
					sql += ", "
				}
			}

			sql += ") VALUES "
			for rDataI, rData := range thisRows {
				sql += "("
				for ic, c := range descriptor.Columns {
					if ic < len(rData.Data) {
						switch c.Type {
						case Tuint, Tenum:
							sql += "?"
							values = append(values, rData.Data[ic].EncodedRawValue)
						case Tstring:
							sql += "?"
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
				sql += ")"
				if rDataI < len(thisRows)-1 {
					sql += ", "
				}
			}
			_, err = db.Exec(sql, values...)
			if err != nil {
				sa.logger.Error("could not insert data", zap.String("sql", sql), zap.Error(err))
				return
			}

			curRow += batchInsertRow
		}
	}

	for _, r := range tableData.Data {
		r.Data = r.Data[0:0]
		sa.rowDataPool.Put(r)
	}

	err = os.Remove(p)
	if err != nil {
		sa.logger.Error("could not delete archive file", zap.String("path", p), zap.Error(err))
		return
	}
}
