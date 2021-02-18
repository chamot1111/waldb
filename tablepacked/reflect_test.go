package tablepacked

import (
	"database/sql"
	"strconv"
	"testing"
)

var reflectTableTest = Table{
	Name: "interaction",
	Columns: []c{
		{Name: "TTString", Type: Tstring},
		{Name: "TTEnumInt", Type: Tenum, EnumValues: []string{"0", "1"}},
		{Name: "TTEnumString", Type: Tenum, EnumValues: []string{"0", "1"}},
		{Name: "TTNullInt", Type: Tuint},
		{Name: "TTInt", Type: Tuint},
		{Name: "TTUint", Type: Tuint},
	},
}

type reflectTest struct {
	TTString     string
	TTEnumInt    int
	TTEnumString string
	TTNullInt    sql.NullInt64
	TTInt        int
	TTUint       uint
}

func TestStructToRow(t *testing.T) {
	rt := &reflectTest{
		TTString:     "1",
		TTEnumInt:    1,
		TTEnumString: "1",
		TTNullInt:    sql.NullInt64{Int64: 1, Valid: true},
		TTInt:        1,
		TTUint:       1,
	}
	row := &RowData{}
	if err := StructToRow(rt, row, reflectTableTest); err != nil {
		t.Fatalf("fail struct to row:%s", err.Error())
	}
	if !equals(rt, row) {
		t.Fatalf("row comparison pb: %v -> %v", rt, row.Data)
	}
}

func TestRowToStruct(t *testing.T) {
	rt := &reflectTest{
		TTString:     "1",
		TTEnumInt:    1,
		TTEnumString: "1",
		TTNullInt:    sql.NullInt64{Int64: 1, Valid: true},
		TTInt:        1,
		TTUint:       1,
	}
	row := &RowData{}
	if err := StructToRow(rt, row, reflectTableTest); err != nil {
		t.Fatalf("fail struct to row:%s", err.Error())
	}
	if !equals(rt, row) {
		t.Fatalf("row comparison pb: %v -> %v", rt, row.Data)
	}

	rtNew := &reflectTest{}
	if err := RowToStruct(rtNew, row, reflectTableTest); err != nil {
		t.Fatalf("fail struct to row:%s", err.Error())
	}
	if !equals(rtNew, row) {
		t.Fatalf("row comparison pb: %v -> %v", rt, row.Data)
	}
}

func equals(rt *reflectTest, row *RowData) bool {
	TTEnumStringInt, _ := strconv.Atoi(rt.TTEnumString)
	return !(string(row.Data[0].Buffer) != rt.TTString ||
		int(row.Data[1].EncodedRawValue) != rt.TTEnumInt ||
		int(row.Data[2].EncodedRawValue) != TTEnumStringInt ||
		int(row.Data[3].EncodedRawValue) != int(rt.TTNullInt.Int64) ||
		int(row.Data[4].EncodedRawValue) != rt.TTInt ||
		uint(row.Data[5].EncodedRawValue) != rt.TTUint)
}
