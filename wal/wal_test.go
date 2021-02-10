package wal

import (
	"bufio"
	"fmt"
	"os"
	"testing"

	"github.com/chamot1111/waldb/config"
	"github.com/chamot1111/waldb/fileop"
	"github.com/chamot1111/waldb/wutils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestWalWriteAndRead(t *testing.T) {
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
		t.Fatalf("%v: %s", entry.Message, entry.Stack)
		return fmt.Errorf("an error happened: %s", entry.Message)
	}))

	sc := config.InitDefaultTestConfig()
	sc.ShardCount = 100

	os.RemoveAll("data-test")

	bfo, err := fileop.InitBucketFileOperationner(*sc, logger)
	if err != nil {
		t.Fatalf("%v", err)
	}
	wal, err := InitWAL(bfo, *sc, 0, logger)
	if err != nil {
		t.Fatalf("%v", err)
	}

	const fileCount = 1000
	const interPerFile = 1000

	fakeRows := make([]byte, interPerFile*20)

	fakeRows[10] = 245
	fakeRows[len(fakeRows)-1] = 240

	cfArray := make([]config.ContainerFile, fileCount)
	for i := 0; i < fileCount; i++ {
		cfArray[i] = config.NewContainerFileWTableName("app", fmt.Sprintf("b%04d", i), "bb1", "interaction")
		err = wal.AppendWrite(cfArray[i], fakeRows)
		if err != nil {
			t.Fatalf("%v", err)
		}
		fileBuf := &wutils.Buffer{}
		err := wal.GetFileBuffer(cfArray[i], fileBuf)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if fileBuf.FullLen() != len(fakeRows) {
			t.Fatalf("should have read %d bytes after init get %d (%s)", len(fakeRows), fileBuf.FullLen(), cfArray[i].Key())
		}
	}
	errOpsCount, err := wal.Flush()
	if err != nil {
		t.Fatalf("%v", err)
	}
	if errOpsCount > 0 {
		t.Fatalf("errOpsCount %v", errOpsCount)
	}

	for i := 0; i < 10; i++ {
		fileBuf := &wutils.Buffer{}
		curI := i
		cf := cfArray[curI%fileCount]
		err := wal.GetFileBuffer(cf, fileBuf)
		if err != nil {
			t.Fatalf("%v", err)
		}

		if fileBuf.FullLen() != len(fakeRows) {
			t.Fatalf("should have read %d bytes get %d (%s) -- %d", len(fakeRows), fileBuf.FullLen(), cf.Key(), curI)
		}

		fileBuf.ResetRead()

		internalBuf := fileBuf.Bytes()
		for k := 0; k < len(internalBuf); k++ {
			if internalBuf[k] != fakeRows[k] {
				t.Fatalf("unequal row")
			}
		}
	}
}

func TestSequentialWrite(t *testing.T) {
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
		t.Fatalf("%v: %s", entry.Message, entry.Stack)
		return fmt.Errorf("an error happened: %s", entry.Message)
	}))

	sc := config.InitDefaultTestConfig()
	sc.ShardCount = 100

	os.RemoveAll("data-test")

	bfo, err := fileop.InitBucketFileOperationner(*sc, logger)
	if err != nil {
		t.Fatalf("%v", err)
	}
	wal, err := InitWAL(bfo, *sc, 0, logger)
	if err != nil {
		t.Fatalf("%v", err)
	}

	const writeCount = 10
	const archiveCount = 10

	fakeRows := make([]byte, 20)

	fakeRows[10] = 245
	fakeRows[len(fakeRows)-1] = 240
	cf := config.NewContainerFileWTableName("app", "b1", "bb1", "interaction")

	for j := 0; j < archiveCount; j++ {
		for i := 0; i < writeCount; i++ {
			err = wal.AppendWrite(cf, fakeRows)
			if err != nil {
				t.Fatalf("%v", err)
			}
			fileBuf := &wutils.Buffer{}
			err := wal.GetFileBuffer(cf, fileBuf)
			if err != nil {
				t.Fatalf("%v", err)
			}
			if fileBuf.FullLen() != len(fakeRows)*(i+1) {
				t.Fatalf("should have read %d bytes after init get %d (%s)", len(fakeRows), fileBuf.FullLen(), cf.Key())
			}
		}
		err = wal.Truncate(cf, 10)
		if err != nil {
			t.Fatalf("%v", err)
		}
		fileBuf := &wutils.Buffer{}
		err := wal.GetFileBuffer(cf, fileBuf)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if fileBuf.FullLen() != 10 {
			t.Fatalf("should have read %d bytes after init get %d (%s)", 10, fileBuf.FullLen(), cf.Key())
		}

		err = wal.Archive(cf)
		if err != nil {
			t.Fatalf("%v", err)
		}
		fileBuf.Reset()
		err = wal.GetFileBuffer(cf, fileBuf)
		if err != nil {
			t.Fatalf("%v", err)
		}
		if fileBuf.FullLen() != 0 {
			t.Fatalf("should have read %d bytes after init get %d (%s)", 0, fileBuf.FullLen(), cf.Key())
		}

		errOps, err := wal.Flush()
		if err != nil {
			t.Fatalf("%v", err)
		}
		if errOps > 0 {
			t.Fatalf("%v", errOps)
		}
	}

}

func TestFlushExistingWalFile(t *testing.T) {
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
		t.Fatalf("%v: %s", entry.Message, entry.Stack)
		return fmt.Errorf("an error happened: %s", entry.Message)
	}))

	os.RemoveAll("data-test")

	fakeRows := make([]byte, 20)

	fakeRows[10] = 245
	fakeRows[len(fakeRows)-1] = 240

	sc := config.InitDefaultTestConfig()
	sc.ShardCount = 100
	cf := config.NewContainerFileWTableName("app", "b1", "bb1", "interaction")

	buffer := &wutils.Buffer{}
	buffer.Write(fakeRows)

	walFile := initFile(0, 0, sc.ShardCount)
	walFile.addCmd(&walCmd{
		cf:             cf,
		cmd:            archiveCmd,
		buffer:         buffer,
		operationIndex: 1,
		retryCount:     0,
	})
	walFile.addCmd(&walCmd{
		cf:             cf,
		cmd:            writeCmd,
		buffer:         buffer,
		writeOffset:    0,
		fileSize:       uint64(len(fakeRows)),
		operationIndex: 2,
		retryCount:     0,
	})
	walFile.addCmd(&walCmd{
		cf:             cf,
		cmd:            truncateCmd,
		operationIndex: 3,
		retryCount:     0,
		writeOffset:    10,
		fileSize:       10,
	})

	os.MkdirAll(sc.WALFolder, 0744)

	file, err := os.OpenFile(getWalPath(*sc, 0), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0744)
	if err != nil {
		t.Fatalf("%v", err)
	}
	writer := bufio.NewWriter(file)

	walFile.writeHeader(writer)
	walFile.writeAllCmdToFile(writer)
	writer.Flush()
	file.Close()

	bfo, err := fileop.InitBucketFileOperationner(*sc, logger)
	if err != nil {
		t.Fatalf("%v", err)
	}
	wal, err := InitWAL(bfo, *sc, 0, logger)
	if err != nil {
		t.Fatalf("%v", err)
	}

	buffer2 := &wutils.Buffer{}
	err = wal.GetFileBuffer(cf, buffer2)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if buffer2.FullLen() != 10 {
		t.Fatalf("old wal file not written: expected %d but get %d", 10, buffer2.FullLen())
	}

	err = wal.AppendWrite(cf, []byte{0, 1, 2})
	if err != nil {
		t.Fatalf("%v", err)
	}

	buffer3 := &wutils.Buffer{}
	err = wal.GetFileBuffer(cf, buffer3)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if buffer3.FullLen() != 10+3 {
		t.Fatalf("old wal file not written: expected %d but get %d", 10+3, buffer3.FullLen())
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
	sc.ShardCount = 100

	os.RemoveAll("data-test")

	bfo, err := fileop.InitBucketFileOperationner(*sc, logger)
	if err != nil {
		b.Fatalf("%v", err)
	}
	wal, err := InitWAL(bfo, *sc, 0, logger)
	if err != nil {
		b.Fatalf("%v", err)
	}

	const fileCount = 1000
	const interPerFile = 1000

	fakeRows := make([]byte, interPerFile*20)

	fakeRows[10] = 245
	fakeRows[len(fakeRows)-1] = 240

	cfArray := make([]config.ContainerFile, fileCount)
	for i := 0; i < fileCount; i++ {
		cfArray[i] = config.NewContainerFileWTableName("app", fmt.Sprintf("b%04d", i), "bb1", "interaction")
		err = wal.AppendWrite(cfArray[i], fakeRows)
		if err != nil {
			b.Fatalf("%v", err)
		}
	}
	errOpsCount, err := wal.Flush()
	if err != nil {
		b.Fatalf("%v", err)
	}
	if errOpsCount > 0 {
		b.Fatalf("errOpsCount %v", errOpsCount)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		fileBuf := &wutils.Buffer{}
		curI := i
		cf := cfArray[curI%fileCount]
		err := wal.GetFileBuffer(cf, fileBuf)
		if err != nil {
			b.Fatalf("%v", err)
		}

		if fileBuf.FullLen() != len(fakeRows) {
			b.Fatalf("should have read %d bytes get %d (%s) -- %d", len(fakeRows), fileBuf.FullLen(), cf.Key(), curI)
		}

		fileBuf.ResetRead()

		internalBuf := fileBuf.Bytes()
		for k := 0; k < len(internalBuf); k++ {
			if internalBuf[k] != fakeRows[k] {
				b.Fatalf("unequal row")
			}
		}
	}

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
	sc.ShardCount = 100

	os.RemoveAll("data-test")

	bfo, err := fileop.InitBucketFileOperationner(*sc, logger)
	if err != nil {
		b.Fatalf("%v", err)
	}
	wal, err := InitWAL(bfo, *sc, 0, logger)
	if err != nil {
		b.Fatalf("%v", err)
	}

	const fileCount = 1000
	const interBatch = 100

	fakeRows := make([]byte, interBatch*20)
	fakeRows[10] = 245
	fakeRows[len(fakeRows)-1] = 240

	cfArray := make([]config.ContainerFile, fileCount)
	for i := 0; i < fileCount; i++ {
		cfArray[i] = config.NewContainerFileWTableName("app", fmt.Sprintf("b%04d", i), "bb1", "interaction")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		curI := i
		cf := cfArray[curI%fileCount]
		err = wal.AppendWrite(cf, fakeRows)
		if err != nil {
			b.Fatalf("%v", err)
		}
	}
}
