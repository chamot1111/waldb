package tablepacked

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/chamot1111/waldb/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestSqliteArchive(t *testing.T) {

	encoderCfg := zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		NameKey:        "logger",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
	}
	core := zapcore.NewCore(zapcore.NewJSONEncoder(encoderCfg), os.Stdout, zap.DebugLevel)
	logger := zap.New(core).WithOptions()
	logger = logger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		return fmt.Errorf("an error happened: %s", entry.Message)
	}))

	sc := config.InitDefaultTestConfig()

	os.RemoveAll("data-test")

	bfo, err := InitDriver(*sc, logger, map[string]Table{
		"interaction": InteractionTable,
	})
	if err != nil {
		t.Fatalf("%v", err)
	}

	cf := config.NewContainerFileWTableName("app", "b1", "bb1", "interaction")

	iii := interactionRnd()
	rows := []*RowData{
		&iii,
	}
	err = bfo.AppendRowData(cf, rows)
	if err != nil {
		t.Fatalf("%v", err)
	}

	err = bfo.Archive(cf)
	if err != nil {
		t.Fatalf("%v", err)
	}

	opsErr, err := bfo.Flush()
	if opsErr > 0 {
		t.Fatalf("Error happened during flush %d", opsErr)
	}
	if err != nil {
		t.Fatalf("Error happened during flush %s", err.Error())
	}

	time.Sleep(2 * time.Second)
	fdb := SqliteDbPathForTableName(*sc, "interaction")
	db, err := sql.Open("sqlite3", fdb)
	rowsSqlite, err := db.Query("SELECT LearningSessionID from interaction")
	if err != nil {
		t.Fatalf("Error happened during select %s", err.Error())
	}

	rowCount := 0
	for rowsSqlite.Next() {
		var ls int
		err := rowsSqlite.Scan(&ls)
		if err != nil {
			log.Fatal(err)
		}
		if ls != 1111 {
			t.Fatalf("bad learning session id %d", ls)
		}
		rowCount++
	}
	if rowCount != 1 {
		t.Fatalf("return %d rows but expected 1", rowCount)
	}
	err = rowsSqlite.Err()
	if err != nil {
		log.Fatal(err)
	}
	rowsSqlite.Close()
}
