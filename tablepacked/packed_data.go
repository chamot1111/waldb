package tablepacked

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/chamot1111/waldb/config"
	"github.com/chamot1111/waldb/wal"
	"github.com/chamot1111/waldb/wutils"
)

// ColumnData is the lowest level representation of a column in binary format
type ColumnData struct {
	// if buffer is not nil, EncodedRawValue is the buffer lenght
	// else the value can be used for value encoding (uint, int, bool, enum, float)
	EncodedRawValue uint64
	Buffer          []byte
}

// RowData row data
type RowData struct {
	Data []ColumnData
}

// TableData table data
type TableData struct {
	Data []*RowData
	refc uint
}

// Refc count
func (t *TableData) Refc() uint {
	return t.refc
}

// TableDataSlice table data slice
type TableDataSlice struct {
	table *TableData
	len   int
}

// AppendColumnData to buffer
func (cd ColumnData) AppendColumnData(buffer []byte) []byte {
	res, _ := writeColumnValueEncoding(buffer, cd.EncodedRawValue, cd.Buffer != nil)
	if cd.Buffer != nil {
		res = append(res, cd.Buffer...)
	}
	return res
}

// InitTableDataSlice from table data
func InitTableDataSlice(t *TableData) TableDataSlice {
	t.refc++
	return TableDataSlice{
		table: t,
		len:   len(t.Data),
	}
}

// Retain rfc count for this table data
func (tds TableDataSlice) Retain() {
	tds.table.refc++
}

// Release rfc count for this table data
func (tds TableDataSlice) Release() {
	if tds.table != nil {
		tds.table.refc--
	}
}

// Len len of the table data slice
func (tds TableDataSlice) Len() int {
	return tds.len
}

// Row get row at index
func (tds TableDataSlice) Row(i int) *RowData {
	if i >= tds.len {
		panic(fmt.Sprintf("out of bound"))
	}
	return tds.table.Data[i]
}

// AllRows get all rows in the slice
func (tds TableDataSlice) AllRows() []*RowData {
	return tds.table.Data[0:tds.len]
}

// WriteToBuffer write row data to buffer
func (rd RowData) WriteToBuffer(buffer []byte) []byte {
	res := buffer
	for _, v := range rd.Data {
		res = v.AppendColumnData(res)
	}
	return res
}

// ReadFromBuffer read row data from buffer
func ReadFromBuffer(buffer []byte, res *RowData) error {
	res.Data = res.Data[0:0]
	bl := len(buffer)
	var n uint = 0
	ci := 0
	fullResData := res.Data[0:cap(res.Data)]
	for true {
		var c *ColumnData
		if ci >= len(fullResData) {
			fullResData = append(fullResData, ColumnData{})
			fullResData = fullResData[0:cap(fullResData)]
		}
		c = &fullResData[ci]
		var err error
		var nn uint
		if nn, err = readColumnData(buffer[n:], c); err != nil {
			return fmt.Errorf("could not read columns data from buffer: %w", err)
		}
		n = n + nn

		if int(n) == bl {
			res.Data = fullResData[0 : ci+1]
			break
		}

		if int(n) > bl {
			return fmt.Errorf("overflow during read buffer")
		}
		ci++
	}
	return nil
}

// ErrBadEndingCRC happened when a row has a bad CRC
// SaneOffset is the limit we should truncate the file to
// have a sane file
type ErrBadEndingCRC struct {
	SaneOffset int
}

func (e *ErrBadEndingCRC) Error() string { return "ErrBadEndingCRC" }

func crcForBuffer(buffer []byte) uint8 {
	var res uint8 = 128
	for i := 0; i < len(buffer); i++ {
		res = res + buffer[i]
	}
	return res
}

func appendRowDataToFile(cf config.ContainerFile, wal *wal.WAL, rows []*RowData) error {
	buffer := make([]byte, 0, 256)
	var bBuffer [128]byte
	var lenBuffer [2]byte

	for _, r := range rows {
		rBinary := r.WriteToBuffer(bBuffer[0:0])
		binary.BigEndian.PutUint16(lenBuffer[:], uint16(len(rBinary)))
		crc := crcForBuffer(rBinary)
		buffer = append(buffer, lenBuffer[:]...)
		buffer = append(buffer, rBinary[:]...)
		buffer = append(buffer, []byte{crc}...)
	}

	return wal.AppendWrite(cf, buffer)
}

func copyFromFileToByteBuffer(file *os.File, buffer *wutils.Buffer) error {
	const minRead = 4096
	var cur int64 = 0
	for {
		buffer.Grow(minRead)
		fullBuf := buffer.Bytes()
		capFullBuf := int64(cap(fullBuf))
		buf := fullBuf[cur:capFullBuf]
		n, err := file.ReadAt(buf, cur)
		if err != nil {
			if err == io.EOF {
				err = buffer.ChangeBufferSize(int(cur) + n)
				return nil
			}
			return err
		}
		cur = cur + int64(n)

		err = buffer.ChangeBufferSize(int(cur))
		if err != nil {
			return err
		}
	}
}

// ReadAllRowDataFromFile append row data
func ReadAllRowDataFromFile(cf config.ContainerFile, wal *wal.WAL, rowDataPool *sync.Pool, bufferPool *sync.Pool) (*TableData, error) {
	var cCRC uint8
	var table *TableData = &TableData{}
	table.Data = table.Data[0:0]
	SaneOffset := 0

	fileBuf := bufferPool.Get().(*wutils.Buffer)
	fileBuf.Reset()

	err := wal.GetFileBuffer(cf, fileBuf)
	if err != nil {
		bufferPool.Put(fileBuf)
		return nil, err
	}

	for true {
		isEOF := false
		lenBufferBytes := fileBuf.Next(2)
		if len(lenBufferBytes) == 0 {
			break
		} else if len(lenBufferBytes) == 1 {
			bufferPool.Put(fileBuf)
			return table, fmt.Errorf("Could not read lenBufferBytes")
		}

		lenBuffer := int(binary.BigEndian.Uint16(lenBufferBytes[:]))
		internalBuffer := fileBuf.Next(lenBuffer)
		rCRC, err := fileBuf.ReadByte()
		if err != nil {
			if err != io.EOF {
				bufferPool.Put(fileBuf)
				return table, err
			}
			isEOF = true
		}

		cCRC = crcForBuffer(internalBuffer)

		if cCRC != rCRC {
			bufferPool.Put(fileBuf)
			return table, &ErrBadEndingCRC{
				SaneOffset: SaneOffset,
			}
		}

		rd := rowDataPool.Get().(*RowData)

		err = ReadFromBuffer(internalBuffer, rd)
		if err != nil {
			bufferPool.Put(fileBuf)
			return table, err
		}

		table.Data = append(table.Data, rd)
		SaneOffset = fileBuf.ReadOffset()

		if isEOF {
			break
		}
	}

	bufferPool.Put(fileBuf)
	return table, nil
}

// ReadAllRowDataFromFileCorruptSafe append row data and repair file if corruption happened
func ReadAllRowDataFromFileCorruptSafe(cf config.ContainerFile, wal *wal.WAL, rowDataPool *sync.Pool, bufferPool *sync.Pool) (*TableData, error) {
	table, err := ReadAllRowDataFromFile(cf, wal, rowDataPool, bufferPool)
	if err != nil {
		if errCrc, ok := err.(*ErrBadEndingCRC); ok {
			err := wal.Truncate(cf, int64(errCrc.SaneOffset))
			if err != nil {
				return table, fmt.Errorf("Could not sanitize crc error, truncate fail: %w", errCrc)
			}
		} else {
			return table, err
		}
	}
	return table, nil
}
