package tablepacked

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"

	"github.com/chamot1111/waldb/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestShardOneWAndR(t *testing.T) {

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

	bfo, err := InitDriver(*sc, logger)
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

	resRows, err := bfo.ReadAllRowData(cf)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if resRows.Len() != 1 {
		t.Fatalf("should have read %d but get %d", 1, resRows.Len())
	}
}

func TestShardOneWAndReadAfterClose(t *testing.T) {

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

	os.RemoveAll("data-test")

	sc := config.InitDefaultTestConfig()

	bfo, err := InitDriver(*sc, logger)
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

	bfo.Close()

	resRows, err := bfo.ReadAllRowData(cf)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if resRows.Len() != 1 {
		t.Fatalf("should have read %d but get %d", 1, resRows.Len())
	}
}

func TestShardCrc(t *testing.T) {

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

	bfo, err := InitDriver(*sc, logger)
	if err != nil {
		t.Fatalf("%v", err)
	}

	cf := config.NewContainerFileWTableName("app", "b1", "bb1", "interaction")

	iii1 := interactionRnd()
	iii2 := interactionRnd()
	rows := []*RowData{
		&iii1,
		&iii2,
	}
	err = bfo.AppendRowData(cf, rows)
	if err != nil {
		t.Fatalf("%v", err)
	}

	bfo.Close()

	path := cf.PathToFile(*sc)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0744)
	if err != nil {
		t.Errorf("could not open file %s: %w", path, err)
	}

	bytes, err := ioutil.ReadAll(bufio.NewReader(file))
	if err != nil {
		t.Errorf("could not open file %s: %w", path, err)
	}

	_, err = file.Seek(0, 0)
	if err != nil {
		t.Errorf("could not open file %s: %w", path, err)
	}

	_, err = file.WriteAt([]byte{15}, int64(len(bytes))-1)
	if err != nil {
		t.Errorf("could not open file %s: %w", path, err)
	}

	err = file.Sync()
	if err != nil {
		t.Errorf("could not open file %s: %w", path, err)
	}

	resRows, err := bfo.ReadAllRowData(cf)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if resRows.Len() != 1 {
		t.Fatalf("should have read %d but get %d", 1, resRows.Len())
	}
}

func BenchmarkReads(b *testing.B) {

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
		b.Fatalf("%v", entry.Message)
		return fmt.Errorf("an error happened: %s", entry.Message)
	}))

	sc := config.InitDefaultTestConfig()

	os.RemoveAll("data-test")

	bfo, err := InitDriver(*sc, logger)
	if err != nil {
		b.Fatalf("%v", err)
	}

	const fileCount = 1000
	const interPerFile = 1000

	fakeRows := make([]*RowData, interPerFile)

	for i := 0; i < interPerFile; i++ {
		v := interactionRnd()
		fakeRows[i] = &v
	}

	cfArray := make([]config.ContainerFile, fileCount)
	for i := 0; i < fileCount; i++ {
		cfArray[i] = config.NewContainerFileWTableName("app", fmt.Sprintf("b%04d", i), "bb1", "interaction")
		err = bfo.AppendRowData(cfArray[i], fakeRows)
		if err != nil {
			b.Fatalf("%v", err)
		}
	}
	errOpsCount, err := bfo.Flush()
	if err != nil {
		b.Fatalf("%v", err)
	}

	if errOpsCount > 0 {
		b.Fatalf("%v", errOpsCount)
	}

	b.ResetTimer()

	var i int64 = 0

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			curI := atomic.AddInt64(&i, 1)
			cf := cfArray[curI%fileCount]
			table, err := bfo.ReadAllRowData(cf)
			if err != nil {
				b.Fatalf("%v", err)
			}

			if table.Len() != interPerFile {
				b.Fatalf("should have read %d interactions in the file but get %d (%s) -- %d", interPerFile, table.Len(), cf.Key(), curI)
			}

			for k := 0; k < table.Len(); k++ {
				r := table.Row(k)
				v := fakeRows[k]

				if len(r.Data) != len(v.Data) {
					b.Fatalf("rows have not same column count: %d != %d", len(r.Data), len(v.Data))
				}
				for s := 0; s < len(r.Data); s++ {
					rr := r.Data[s]
					vv := v.Data[s]
					if rr.EncodedRawValue != vv.EncodedRawValue || len(rr.Buffer) != len(vv.Buffer) {
						b.Fatalf("unequal row")
					}
				}
			}
			table.Release()
			bfo.FreeTable(table)
		}
	})
}

func BenchmarkWrites(b *testing.B) {

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
		b.Fatalf("%v", entry.Message)
		return fmt.Errorf("an error happened: %s", entry.Message)
	}))

	sc := config.InitDefaultTestConfig()

	os.RemoveAll("data-test")

	bfo, err := InitDriver(*sc, logger)
	if err != nil {
		b.Fatalf("%v", err)
	}

	const fileCount = 1000
	const interBatch = 100

	fakeRows := make([]*RowData, interBatch)

	for i := 0; i < interBatch; i++ {
		v := interactionRnd()
		fakeRows[i] = &v
	}

	cfArray := make([]config.ContainerFile, fileCount)
	for i := 0; i < fileCount; i++ {
		cfArray[i] = config.NewContainerFileWTableName("app", fmt.Sprintf("b%04d", i), "bb1", "interaction")
	}

	b.ResetTimer()

	var i int64 = 0

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			curI := atomic.AddInt64(&i, 1)
			cf := cfArray[curI%fileCount]
			err = bfo.AppendRowData(cf, fakeRows)
			if err != nil {
				b.Fatalf("%v", err)
			}
		}
	})
}
