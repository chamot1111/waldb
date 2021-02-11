package config

// Config config
type Config struct {
	ActiveFolder             string
	ArchiveFolder            string
	WalArchiveFolder         string
	ReplicationActiveFolder  string
	ReplicationArchiveFolder string
	ShardCount               int
	MaxFileOpen              int
	WALFolder                string
	MaxWALFileSize           int
	MaxWALFileDurationS      int
}

// InitDefaultConfig init config with default parameters
func InitDefaultConfig() *Config {
	return &Config{
		ActiveFolder:        "data/active",
		ArchiveFolder:       "data/archive",
		WalArchiveFolder:    "data/wal-archive",
		ShardCount:          4,
		MaxFileOpen:         100,
		WALFolder:           ".",
		MaxWALFileSize:      16000000,
		MaxWALFileDurationS: 10 * 60,
	}
}

// InitDefaultTestConfig init config for test
func InitDefaultTestConfig() *Config {
	return &Config{
		ActiveFolder:             "data-test/active",
		ArchiveFolder:            "data-test/archive",
		WalArchiveFolder:         "data-test/wal-archive",
		ReplicationActiveFolder:  "data-test/replication/active",
		ReplicationArchiveFolder: "data-test/replication/archive",
		ShardCount:               8,
		MaxFileOpen:              10,
		WALFolder:                "data-test",
		MaxWALFileSize:           16000000,
		MaxWALFileDurationS:      1000000000000,
	}
}
