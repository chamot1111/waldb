package config

import (
	"testing"
)

func TestArchiveParser(t *testing.T) {
	conf := InitDefaultTestConfig()
	cf := NewContainerFileWTableName("cont", "b0", "bb0", "table")

	archP := cf.ArchivePath(conf.ArchiveFolder, 1, 2, 3)

	nCf, err := ParseContainerFileFromArchivePath(archP)
	if err != nil {
		t.Fatalf("could not parse archive path: %s", err.Error())
	}

	if cf.Container != nCf.Container || cf.Bucket != nCf.Bucket || cf.SubBucket != nCf.SubBucket || cf.TableName != nCf.TableName {
		t.Fatalf("not equals: %+v -> %+v", cf, nCf)
	}
}

func TestActiveParser(t *testing.T) {
	conf := InitDefaultTestConfig()
	cf := NewContainerFileWTableName("cont", "b0", "bb0", "table")

	archP := cf.PathToFile(*conf)

	nCf, err := ParseContainerFileFromActivePath(archP)
	if err != nil {
		t.Fatalf("could not parse archive path: %s", err.Error())
	}

	if cf.Container != nCf.Container || cf.Bucket != nCf.Bucket || cf.SubBucket != nCf.SubBucket || cf.TableName != nCf.TableName {
		t.Fatalf("not equals: %+v -> %+v", cf, nCf)
	}
}
