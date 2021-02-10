package tablepacked

import (
	"errors"
	"fmt"
	"sync"

	"github.com/buger/jsonparser"
	"github.com/chamot1111/waldb/wutils"
)

func (rd RowData) appendToJSON(table Table, inputBuffer *wutils.Buffer) error {
	var err error
	var logError error
	var res = inputBuffer

	grow := 2
	for i := 0; i < len(table.Columns); i++ {
		c := table.Columns[i]
		colData := rd.Data[i]
		if colData.Buffer != nil && len(colData.Buffer) == 0 {
			continue
		}
		v := c.maxLenJSONValue(colData)
		if v < 0 {
			return fmt.Errorf("col has no max size: %d", c.Type)
		}
		grow = grow + 4 + v
	}
	res.Grow(grow)

	res.WriteUnsafeByte('{')
	for i := 0; i < len(table.Columns); i++ {
		c := table.Columns[i]
		colData := rd.Data[i]
		if colData.Buffer != nil && len(colData.Buffer) == 0 {
			continue
		}
		res.WriteUnsafeByte('"')
		res.WriteString(c.JSONKey)
		res.WriteUnsafeByte('"')
		res.WriteUnsafeByte(':')
		err = c.appendToJSONValue(res, colData)
		if err != nil {
			logError = fmt.Errorf("json row export contains errors: %w", err)
		}
		if i < len(table.Columns)-1 {
			res.WriteUnsafeByte(',')
		}
	}
	res.WriteUnsafeByte('}')
	return logError
}

// RowsDataToJSON rows data to JSON
func RowsDataToJSON(rowsData []*RowData, table Table, bufferPool *sync.Pool) (*wutils.Buffer, error) {
	var err error
	var logError error
	res := bufferPool.Get().(*wutils.Buffer)
	res.Reset()
	res.WriteByte('[')
	for i := 0; i < len(rowsData); i++ {
		rd := rowsData[i]
		err = rd.appendToJSON(table, res)
		if err != nil {
			logError = fmt.Errorf("json table export contains errors: %w", err)
		}
		if i < len(rowsData)-1 {
			res.WriteByte(',')
		}
	}
	res.WriteByte(']')
	return res, logError
}

// ParseJSON parse json to row data
func (t Table) ParseJSON(buffer []byte) (RowData, error) {
	res := RowData{
		Data: make([]ColumnData, len(t.Columns)),
	}
	errors := make([]error, len(t.Columns))
	hasError := false

	parsed := make([]bool, len(t.Columns))
	nullValue := make([]bool, len(t.Columns))
	paths := make([][]string, len(t.Columns))
	for i, v := range t.Columns {
		paths[i] = []string{v.JSONKey}
	}

	jsonparser.EachKey(buffer, func(idx int, value []byte, vt jsonparser.ValueType, err error) {
		if err != nil {
			hasError = true
			errors[idx] = err
			return
		}
		if vt == jsonparser.String {
			if t.Columns[idx].Type == Tenum {
				cc := t.Columns[idx]
				sv := string(value)
				for i := 0; i < len(cc.EnumValues); i++ {
					if sv == cc.EnumValues[i] {
						res.Data[idx].EncodedRawValue = uint64(i)
						parsed[idx] = true
						return
					}
				}
				res.Data[idx].EncodedRawValue = 0
				res.Data[idx].Buffer = []byte{}
				nullValue[idx] = true
			} else {
				res.Data[idx].EncodedRawValue = uint64(len(value))
				dst := make([]byte, len(value))
				copy(dst, value)
				res.Data[idx].Buffer = dst
				parsed[idx] = true
			}
		} else {
			var perr error
			v, perr := jsonparser.ParseInt(value)
			if perr != nil {
				hasError = true
				errors[idx] = perr
			}
			res.Data[idx].EncodedRawValue = uint64(v)
			parsed[idx] = true
		}
	}, paths...)

	if hasError {
		errStrWrap := "multiple errors happened: "
		realErrors := make([]interface{}, 0)
		for _, v := range errors {
			if v != nil {
				errStrWrap = errStrWrap + " %w,"
				realErrors = append(realErrors, v)
			}
		}
		return res, fmt.Errorf(errStrWrap, realErrors...)
	}

	for i := 0; i < len(t.Columns); i++ {
		c := t.Columns[i]
		p := parsed[i]
		nv := nullValue[i]
		if c.JSONMandatory && !p {
			return res, fmt.Errorf("column %s is mandatory", c.JSONKey)
		}
		if c.NotNullable && nv {
			return res, fmt.Errorf("column %s is not nullable", c.JSONKey)
		}
	}

	return res, nil
}

func (c ColumnDescriptor) maxLenJSONValue(cd ColumnData) int {
	switch c.Type {
	case Tuint:
		return wutils.MaxIntStrLen
	case Tenum:
		return len(c.EnumValues[int(cd.EncodedRawValue)])
	case Tstring:
		if cd.Buffer == nil {
			return 4 // null
		}
		return len(cd.Buffer) * 2 // '"' escape
	}
	return -1
}

func (c ColumnDescriptor) appendToJSONValue(inputBuffer *wutils.Buffer, cd ColumnData) error {
	res := inputBuffer
	switch c.Type {
	case Tuint:
		res.WriteIntAsString(int64(cd.EncodedRawValue))
	case Tenum:
		res.WriteUnsafeByte('"')
		res.WriteString(c.EnumValues[int(cd.EncodedRawValue)])
		res.WriteUnsafeByte('"')
		if int(cd.EncodedRawValue) > len(c.EnumValues) {
			return errors.New("Enum value is not in possibilities")
		}
	case Tstring:
		if cd.Buffer == nil {
			res.WriteString("null")
			return errors.New("Enum value is not in possibilities")
		}
		res.WriteByte('"')
		for i := 0; i < len(cd.Buffer); i++ {
			b := cd.Buffer[i]
			if b == '"' {
				res.WriteUnsafeByte('\\')
			} else if b == '\\' {
				res.WriteUnsafeByte('\\')
			}
			res.WriteUnsafeByte(b)
		}
		res.WriteUnsafeByte('"')
	}
	return nil
}
