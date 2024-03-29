package wal

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/chamot1111/waldb/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestReplication(t *testing.T) {
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

	conf := config.InitDefaultTestConfig()
	conf.ReplicationActiveFolder = "data-test/rep-active"
	conf.ReplicationArchiveFolder = "data-test/rep-archive"
	conf.ArchiveCommand = "echo \"%f %p\" > data-test/rsyn-test.txt"

	os.RemoveAll("data-test")

	shardWal, err := InitShardWAL(*conf, logger, nil)
	if err != nil {
		t.Fatalf("%v", err)
	}

	rep := InitReplicator(shardWal.GetArchiveEventChan(), conf.ReplicationActiveFolder, conf.ReplicationArchiveFolder, "", logger)
	rep.Start()

	cf := config.NewContainerFileWTableName("app1", "b0", "sb0", "inter")

	si := cf.ShardIndex(uint32(conf.ShardCount))
	shardWal.LockShardIndex(si)
	wal := shardWal.GetWalForShardIndex(si)
	buf := [3]byte{1, 2, 3}
	wal.AppendWrite(cf, buf[:])
	shardWal.UnlockShardIndex(si)

	errors := shardWal.CloseAll()
	if errors != nil && errors.Err() != nil {
		t.Fatalf("%v", errors.Err())
	}

	rep.Stop()

	contentB, err := ioutil.ReadFile(cf.PathToFileFromFolder("data-test/rep-active"))
	if err != nil {
		t.Fatalf("%v", err)
	}

	if contentB[len(contentB)-3] != 1 || contentB[len(contentB)-2] != 2 || contentB[len(contentB)-1] != 3 {
		t.Fatalf("%v", err)
	}
}

func TestArchiveCmd(t *testing.T) {
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

	conf := config.InitDefaultTestConfig()
	conf.ReplicationActiveFolder = "data-test/rep-active"
	conf.ArchiveCommand = "echo \"%f %p\" > data-test/rsyn-test.txt"

	os.RemoveAll("data-test")

	shardWal, err := InitShardWAL(*conf, logger, nil)
	if err != nil {
		t.Fatalf("%v", err)
	}

	rep := InitReplicator(shardWal.GetArchiveEventChan(), conf.ReplicationActiveFolder, conf.ReplicationArchiveFolder, conf.ArchiveCommand, logger)
	rep.Start()

	cf := config.NewContainerFileWTableName("app1", "b0", "sb0", "inter")

	si := cf.ShardIndex(uint32(conf.ShardCount))
	shardWal.LockShardIndex(si)
	wal := shardWal.GetWalForShardIndex(si)
	buf := [3]byte{1, 2, 3}
	wal.AppendWrite(cf, buf[:])
	shardWal.UnlockShardIndex(si)

	errors := shardWal.CloseAll()
	if errors != nil && errors.Err() != nil {
		t.Fatalf("%v", errors.Err())
	}

	rep.Stop()

	contentB, err := ioutil.ReadFile("data-test/rsyn-test.txt")
	if err != nil {
		t.Fatalf("%v", err)
	}
	content := strings.TrimSuffix(string(contentB), "\n")
	var comps = strings.Split(content, " ")
	if len(comps) != 2 {
		t.Fatalf("should have 2 comps: %v", content)
	}
	var filename = comps[0]
	var fullpath = comps[1]

	expectedContent := conf.WalArchiveFolder + "/" + filename
	if fullpath != expectedContent {
		t.Fatalf("'%s' expected '%s'", content, expectedContent)
	}
}

func TestRsyncCmd(t *testing.T) {
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

	conf := config.InitDefaultTestConfig()
	conf.ShardCount = 100
	conf.RsyncCommand = "echo \"%act %arc %walact %walarc %cust\" > data-test/rsyn-test.txt"

	os.RemoveAll("data-test")

	shardWal, err := InitShardWAL(*conf, logger, nil)
	if err != nil {
		t.Fatalf("%v", err)
	}

	_, err = shardWal.ExecRsyncCommand(map[string][]string{"cust": {"test"}})
	if err != nil {
		t.Fatalf("%v", err)
	}

	contentB, err := ioutil.ReadFile("data-test/rsyn-test.txt")
	if err != nil {
		t.Fatalf("%v", err)
	}
	content := strings.TrimSuffix(string(contentB), "\n")

	expectedContent := conf.ActiveFolder + " " + conf.ArchiveFolder + " " + conf.WALFolder + " " + conf.WalArchiveFolder + " test"
	if content != expectedContent {
		t.Fatalf("'%s' expected '%s'", content, expectedContent)
	}
}
