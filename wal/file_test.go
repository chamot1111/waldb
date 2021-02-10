package wal

import (
	"bufio"
	"fmt"
	"os"
	"testing"

	"github.com/chamot1111/waldb/config"
	"github.com/chamot1111/waldb/wutils"
)

func TestSaveAndReload(t *testing.T) {
	wf := initFile(0, 0, 1)

	cmd1 := &walCmd{
		cf:          config.NewContainerFileWTableName("app1", "b0", "sb0", "inter"),
		cmd:         writeCmd,
		buffer:      wutils.NewBuffer([]byte("hello")),
		writeOffset: 0,
		fileSize:    5,
	}
	wf.addCmd(cmd1)

	cmd2 := &walCmd{
		cf:          config.NewContainerFileWTableName("app1", "b0", "sb0", "inter"),
		cmd:         truncateCmd,
		buffer:      nil,
		writeOffset: 0,
		fileSize:    0,
	}
	wf.addCmd(cmd2)

	cmd3 := &walCmd{
		cf:          config.NewContainerFileWTableName("app1", "b0", "sb0", "inter"),
		cmd:         archiveCmd,
		buffer:      nil,
		writeOffset: 0,
		fileSize:    0,
	}
	wf.addCmd(cmd3)

	f, err := os.OpenFile("test-wal.bin", os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0744)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if f != nil {
			f.Close()
			os.Remove("test-wal.bin")
		}
	}()

	writer := bufio.NewWriter(f)

	err = wf.writeHeader(writer)
	if err != nil {
		t.Fatal(err)
	}

	err = wf.writeAllCmdToFile(writer)
	if err != nil {
		t.Fatal(err)
	}

	writer.Flush()
	f.Close()
	f = nil

	wf2, err := ReadFileFromPath("test-wal.bin")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("%v", wf2)
	err = walFileEquals(wf, wf2)
	if err != nil {
		t.Fatal(err)
	}
}

func walFileEquals(w1, w2 *File) error {
	if len(w1.cmdsOrder) != len(w2.cmdsOrder) {
		return fmt.Errorf("not the same count of cmds")
	}

	for i := 0; i < len(w1.cmdsOrder); i++ {
		if w1.cmdsOrder[i].cmd != w2.cmdsOrder[i].cmd {
			return fmt.Errorf("not the same cmd for %d", i)
		}
		if w1.cmdsOrder[i].writeOffset != w2.cmdsOrder[i].writeOffset {
			return fmt.Errorf("not the same offset for %d", i)
		}
		if w1.cmdsOrder[i].fileSize != w2.cmdsOrder[i].fileSize {
			return fmt.Errorf("not the same fileSize for %d", i)
		}
		if w1.cmdsOrder[i].cf.Key() != w2.cmdsOrder[i].cf.Key() {
			return fmt.Errorf("not the same container file for %d", i)
		}
		if err := bufferAreEquals(w1.cmdsOrder[i].buffer, w2.cmdsOrder[i].buffer); err != nil {
			return fmt.Errorf("not the same buffer %d: %w", i, err)
		}
	}
	return nil
}

func bufferAreEquals(b1, b2 *wutils.Buffer) error {
	if (b1 == nil && b2 != nil) || (b2 == nil && b1 != nil) {
		return fmt.Errorf("one is nil")
	}
	if b1 == nil {
		return nil
	}
	b1.ResetRead()
	b2.ResetRead()
	if b1.Len() != b2.Len() {
		return fmt.Errorf("not same len")
	}
	bb1 := b1.Bytes()
	bb2 := b2.Bytes()
	for i := 0; i < len(bb1); i++ {
		if bb1[i] != bb2[i] {
			return fmt.Errorf("not same value at byte %d", i)
		}
	}
	return nil
}
