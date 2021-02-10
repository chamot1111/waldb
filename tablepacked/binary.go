package tablepacked

import (
	"encoding/binary"
	"errors"
)

var errOverflow = errors.New("binary: varint overflows a 64-bit integer")

func readUvarint(r []byte) (uint64, uint8, error) {
	var x uint64
	var s uint
	var n uint8 = 0
	var idxB int = 0
	for i := 0; i < binary.MaxVarintLen64; i++ {
		var err error
		var b byte
		if idxB >= len(r) {
			err = errOverflow
		} else {
			b = r[idxB]
			idxB++
		}
		n = n + 1
		if err != nil {
			return x, n, err
		}
		if b < 0x80 {
			if i == 9 && b > 1 {
				return x, n, errOverflow
			}
			return x | uint64(b)<<s, n, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return x, n, errOverflow
}

func readColumnData(buffer []byte, c *ColumnData) (uint, error) {
	ux, n, err := readUvarint(buffer) // ok to continue in presence of error
	isBufferLengthEncoding := ux & 1
	value := uint64(ux >> 1)
	c.EncodedRawValue = value
	if isBufferLengthEncoding != 0 {
		c.Buffer = buffer[n : uint64(n)+value]
		return uint(n) + uint(value), err
	}
	c.Buffer = nil
	return uint(n), err
}

func writeColumnValueEncoding(buffer []byte, value uint64, isBuffer bool) ([]byte, int) {
	var tmpBuffer [binary.MaxVarintLen64]byte
	ux := uint64(value) << 1
	if isBuffer {
		ux = ux | 0x1
	}
	n := binary.PutUvarint(tmpBuffer[:], ux)
	return append(buffer, tmpBuffer[0:n]...), n
}
