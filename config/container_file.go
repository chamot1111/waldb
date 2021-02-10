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

// NewContainerFileWTableName new container file
func NewContainerFileWTableName(container string, bucket string, subBucket string, table string) ContainerFile {
	return ContainerFile{Container: container, Bucket: bucket, SubBucket: subBucket, TableName: table}
}

// PathToFile path to file
func (cf ContainerFile) PathToFile(c Config) string {
	return path.Join(c.ActiveFolder, cf.Container, cf.Bucket, cf.SubBucket, cf.TableName)
}

// PathToFileFromFolder path to file
func (cf ContainerFile) PathToFileFromFolder(folder string) string {
	return path.Join(folder, cf.Container, cf.Bucket, cf.SubBucket, cf.TableName)
}

// BaseFolder to file
func (cf ContainerFile) BaseFolder(c Config) string {
	return path.Join(c.ActiveFolder, cf.Container, cf.Bucket, cf.SubBucket)
}

// Key string representing the config
func (cf ContainerFile) Key() string {
	return strings.Join([]string{cf.Container, cf.Bucket, cf.SubBucket, cf.TableName}, ":")
}

// ArchivePath path to the arvhive file
func (cf ContainerFile) ArchivePath(archiveFolder string, shardIndex, walIndex, operationIndex int) string {
	v := fmt.Sprintf("%s-%d-%d-%d", cf.TableName, shardIndex, walIndex, operationIndex)
	return path.Join(archiveFolder, cf.Container, cf.Bucket, cf.SubBucket, v)
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
