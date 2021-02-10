package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/chamot1111/waldb/config"
)

const persistentStateFilename = "state.bin"

// PersistentState save information needed between restart
type PersistentState struct {
	WalIndex uint64
	file     *os.File
}

// Save state to file
func (p *PersistentState) Save() error {
	var buffer [8]byte
	binary.BigEndian.PutUint64(buffer[:], p.WalIndex)
	_, err := p.file.WriteAt(buffer[:], 0)
	return err
}

// InitPersistentFileFromDisk restore from file
func InitPersistentFileFromDisk(config config.Config, shardIndex int) (*PersistentState, error) {
	file, err := os.OpenFile(persistentStatePath(config, shardIndex), os.O_CREATE|os.O_RDWR, 0744)
	if err != nil {
		return nil, err
	}
	var buffer [8]byte
	for i := 0; i < len(buffer); i++ {
		buffer[i] = 0
	}
	n, err := file.ReadAt(buffer[:], 0)
	if err != nil {
		if err != io.EOF || n != 0 {
			return nil, fmt.Errorf("could not open PersistentState: %w", err)
		}
	}
	return &PersistentState{
		file:     file,
		WalIndex: binary.BigEndian.Uint64(buffer[:]),
	}, nil
}

func persistentStatePath(config config.Config, shardIndex int) string {
	return path.Join(config.WALFolder, fmt.Sprintf("state-%05d.bin", shardIndex))
}
