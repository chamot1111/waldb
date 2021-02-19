package config

import (
	"fmt"
	"hash/fnv"
	"path"
	"strings"
)

// ContainerFile container file
type ContainerFile struct {
	Container string
	Bucket    string
	SubBucket string
	TableName string
}

const digitsPrefix = 4

// NewContainerFileWTableName new container file
func NewContainerFileWTableName(container string, bucket string, subBucket string, table string) ContainerFile {
	return ContainerFile{Container: container, Bucket: bucket, SubBucket: subBucket, TableName: table}
}

// DataSize path to file
func (cf ContainerFile) DataSize() int {
	return len(cf.Container) + len(cf.Bucket) + len(cf.SubBucket) + len(cf.TableName)
}

// PathToFile path to file
func (cf ContainerFile) PathToFile(c Config) string {
	prefix, filename := cf.prefixAndFilename()
	return path.Join(c.ActiveFolder, cf.Container, prefix, filename)
}

// PathToFileFromFolder path to file
func (cf ContainerFile) PathToFileFromFolder(folder string) string {
	prefix, filename := cf.prefixAndFilename()
	return path.Join(folder, cf.Container, prefix, filename)
}

// BaseFolder to file
func (cf ContainerFile) BaseFolder(c Config) string {
	prefix, _ := cf.prefixAndFilename()
	return path.Join(c.ActiveFolder, cf.Container, prefix)
}

func (cf ContainerFile) prefixAndFilename() (prefix string, filename string) {
	prefix = cf.Bucket
	if len(cf.Bucket) > digitsPrefix {
		prefix = cf.Bucket[0:digitsPrefix]
	}
	filename = cf.Bucket + "_" + cf.SubBucket + "_" + cf.TableName
	return
}

// Key string representing the config
func (cf ContainerFile) Key() string {
	return strings.Join([]string{cf.Container, cf.Bucket, cf.SubBucket, cf.TableName}, ":")
}

// ArchivePath path to the archive folder
func (cf ContainerFile) ArchivePath(archiveFolder string, shardIndex, walIndex, operationIndex int) string {
	prefix, filename := cf.prefixAndFilename()
	v := fmt.Sprintf("%s-%d-%d-%d", filename, shardIndex, walIndex, operationIndex)
	return path.Join(archiveFolder, cf.Container, prefix, cf.SubBucket, v)
}

// ArchiveFolder path to the archive file
func (cf ContainerFile) ArchiveFolder(archiveFolder string) string {
	prefix, _ := cf.prefixAndFilename()
	return path.Join(archiveFolder, cf.Container, prefix)
}

// ParseContainerFileKey parse a container file from string
func ParseContainerFileKey(b string) (*ContainerFile, error) {
	comps := strings.Split(string(b), ":")
	if len(comps) != 4 {
		return nil, fmt.Errorf("could not parse container file key '%s'", string(b))
	}
	return &ContainerFile{
		Container: comps[0],
		Bucket:    comps[1],
		SubBucket: comps[2],
		TableName: comps[3],
	}, nil
}

// ShardIndex compute the shard index for a container file
func (cf ContainerFile) ShardIndex(shardCount uint32) uint32 {
	return hash(cf.Container+":"+cf.Bucket) % shardCount
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
