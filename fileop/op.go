package fileop

import (
	"github.com/chamot1111/waldb/config"
	"github.com/chamot1111/waldb/wutils"
)

type OpKind uint8

const (
	WriteOp    OpKind = iota
	ArchiveOp  OpKind = iota
	TruncateOp OpKind = iota
)

type FileBatchOp struct {
	ContainerFile config.ContainerFile
	Ops           []Op
}

type Op struct {
	OpKind          OpKind
	Buffer          *wutils.Buffer
	Offset          uint64
	FileSize        int64
	OperationIndex  uint64
	ArchiveFileName string
}
