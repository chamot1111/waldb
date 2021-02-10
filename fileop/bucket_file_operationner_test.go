package fileop

import (
	"fmt"
	"os"
	"testing"

	"github.com/chamot1111/waldb/config"
	"github.com/chamot1111/waldb/wutils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestOneWAndR(t *testing.T) {

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

	bfo, err := InitBucketFileOperationner(*sc, logger)
	if err != nil {
		t.Fatalf("%v", err)
	}

	cf := config.NewContainerFileWTableName("app", "b1", "bb1", "inter")

	fakeRows := make([]byte, 20)

	err = bfo.WriteAt(&cf, fakeRows, 0, int64(len(fakeRows)))
	if err != nil {
		t.Fatalf("%v", err)
	}

	fileBuf := &wutils.Buffer{}

	err = bfo.GetFileBuffer(&cf, fileBuf)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if fileBuf.FullLen() != 20 {
		t.Fatalf("should have read %d but get %d", 20, fileBuf.FullLen())
	}

	bfo.Close()
}

func TestTruncate(t *testing.T) {

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

	bfo, err := InitBucketFileOperationner(*sc, logger)
	if err != nil {
		t.Fatalf("%v", err)
	}

	cf := config.NewContainerFileWTableName("app", "b1", "bb1", "inter")

	fakeRows := make([]byte, 20)

	err = bfo.WriteAt(&cf, fakeRows, 0, int64(len(fakeRows)))
	if err != nil {
		t.Fatalf("%v", err)
	}

	fileBuf := &wutils.Buffer{}

	err = bfo.GetFileBuffer(&cf, fileBuf)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if fileBuf.FullLen() != 20 {
		t.Fatalf("should have read %d but get %d", 20, fileBuf.FullLen())
	}

	err = bfo.Truncate(&cf, 10)
	if err != nil {
		t.Fatalf("%v", err)
	}

	err = bfo.GetFileBuffer(&cf, fileBuf)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if fileBuf.FullLen() != 10 {
		t.Fatalf("should have read %d but get %d", 10, fileBuf.FullLen())
	}

	n, err := bfo.CurFileSize(&cf)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if n != 10 {
		t.Fatalf("should have read CurFileSize %d but get %d", 10, n)
	}

	bfo.Close()
}

func TestArchive(t *testing.T) {

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

	bfo, err := InitBucketFileOperationner(*sc, logger)
	if err != nil {
		t.Fatalf("%v", err)
	}

	cf := config.NewContainerFileWTableName("app", "b1", "bb1", "inter")

	fakeRows := make([]byte, 20)

	err = bfo.WriteAt(&cf, fakeRows, 0, int64(len(fakeRows)))
	if err != nil {
		t.Fatalf("%v", err)
	}

	fileBuf := &wutils.Buffer{}

	err = bfo.GetFileBuffer(&cf, fileBuf)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if fileBuf.FullLen() != 20 {
		t.Fatalf("should have read %d but get %d", 20, fileBuf.FullLen())
	}

	const shardIndex = 0
	const walIndex = 1
	const operationIndex = 2

	err = bfo.Archive(&cf, shardIndex, walIndex, operationIndex)
	if err != nil {
		t.Fatalf("%v", err)
	}

	n, err := bfo.CurFileSize(&cf)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if n != 0 {
		t.Fatalf("should have read %d but get %d", 0, n)
	}

	archivePath := cf.ArchivePath(sc.ArchiveFolder, shardIndex, walIndex, operationIndex)

	s, err := os.Stat(archivePath)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if s.Size() != 20+headerSize {
		t.Fatalf("should have read %d but get %d", 20+headerSize, s.Size())
	}

	// another archive should change nothing
	err = bfo.Archive(&cf, shardIndex, walIndex, operationIndex)
	if err != nil {
		t.Fatalf("%v", err)
	}

	s, err = os.Stat(archivePath)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if s.Size() != 20+headerSize {
		t.Fatalf("should have read %d but get %d", 20+headerSize, s.Size())
	}

	bfo.Close()
}

func TestApplyBatchOp(t *testing.T) {

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

	bfo, err := InitBucketFileOperationner(*sc, logger)
	if err != nil {
		t.Fatalf("%v", err)
	}

	cf := config.NewContainerFileWTableName("app", "b1", "bb1", "inter")

	fakeRows := make([]byte, 20)
	buffer := &wutils.Buffer{}
	_, err = buffer.Write(fakeRows)
	if err != nil {
		t.Fatalf("%v", err)
	}

	fileBatchOp := []*FileBatchOp{
		{
			ContainerFile: cf,
			Ops: []Op{
				{
					OpKind:         WriteOp,
					Buffer:         buffer,
					Offset:         0,
					FileSize:       20,
					OperationIndex: 0,
				},
				{
					OpKind:         TruncateOp,
					Offset:         10,
					FileSize:       10,
					OperationIndex: 1,
				},
				{
					OpKind:          ArchiveOp,
					FileSize:        10,
					OperationIndex:  2,
					ArchiveFileName: cf.ArchivePath(sc.ArchiveFolder, 0, 0, 2),
				},
			},
		},
	}

	errors := bfo.ApplyBatchOp(fileBatchOp)
	if errors.Err() != nil {
		t.Fatalf("%v", errors.Err())
	}

	fileBuf := &wutils.Buffer{}

	err = bfo.GetFileBuffer(&cf, fileBuf)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if fileBuf.FullLen() != 0 {
		t.Fatalf("should have read %d but get %d", 0, fileBuf.FullLen())
	}

	archivePath := cf.ArchivePath(sc.ArchiveFolder, 0, 0, 2)

	s, err := os.Stat(archivePath)
	if err != nil {
		t.Fatalf("%v", err)
	}

	if s.Size() != 10+headerSize {
		t.Fatalf("should have read %d but get %d", 10+headerSize, s.Size())
	}

	bfo.Close()
}

func TestLimitOpenFile(t *testing.T) {

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
	const countFile = 10
	sc.MaxFileOpen = 2

	bfo, err := InitBucketFileOperationner(*sc, logger)
	if err != nil {
		t.Fatalf("%v", err)
	}

	for i := 0; i < countFile; i++ {
		cf := config.NewContainerFileWTableName("app", fmt.Sprintf("b%d", i), "bb1", "inter")

		fakeRows := make([]byte, 20)

		err = bfo.WriteAt(&cf, fakeRows, 0, int64(len(fakeRows)))
		if err != nil {
			t.Fatalf("%v", err)
		}
	}

	for i := 0; i < countFile; i++ {
		fileBuf := &wutils.Buffer{}
		cf := config.NewContainerFileWTableName("app", fmt.Sprintf("b%d", i), "bb1", "inter")

		err = bfo.GetFileBuffer(&cf, fileBuf)
		if err != nil {
			t.Fatalf("%v", err)
		}

		if fileBuf.FullLen() != 20 {
			t.Fatalf("should have read %d but get %d", 20, fileBuf.FullLen())
		}

		if err := bfo.Sync(&cf); err != nil {
			t.Fatalf("could not sync %s", err.Error())
		}
	}

	bfo.Close()
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

	bfo, err := InitBucketFileOperationner(*sc, logger)
	if err != nil {
		b.Fatalf("%v", err)
	}

	const fileCount = 1000
	const interPerFile = 1000

	fakeRows := make([]byte, interPerFile*20)

	cfArray := make([]config.ContainerFile, fileCount)
	for i := 0; i < fileCount; i++ {
		cfArray[i] = config.NewContainerFileWTableName("app", fmt.Sprintf("b%04d", i), "bb1", "interaction")
		err = bfo.WriteAt(&cfArray[i], fakeRows, 0, int64(len(fakeRows)))
		if err != nil {
			b.Fatalf("%v", err)
		}
		err = bfo.Sync(&cfArray[i])
		if err != nil {
			b.Fatalf("%v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		fileBuf := &wutils.Buffer{}
		curI := i
		cf := cfArray[curI%fileCount]
		err := bfo.GetFileBuffer(&cf, fileBuf)
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

	bfo, err := InitBucketFileOperationner(*sc, logger)
	if err != nil {
		b.Fatalf("%v", err)
	}

	const fileCount = 1000
	const interBatch = 100

	fakeRows := make([]byte, interBatch*20)

	cfArray := make([]config.ContainerFile, fileCount)
	for i := 0; i < fileCount; i++ {
		cfArray[i] = config.NewContainerFileWTableName("app", fmt.Sprintf("b%04d", i), "bb1", "interaction")
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		curI := i
		cf := cfArray[curI%fileCount]
		err = bfo.WriteAt(&cf, fakeRows, 0, int64(len(fakeRows)))
		if err != nil {
			b.Fatalf("%v", err)
		}
	}
}
