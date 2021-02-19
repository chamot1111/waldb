package tablepacked

import (
	"encoding/json"
	"io/ioutil"
)

// DataType type of static column
type DataType uint8

const (
	// Tuint uint type
	Tuint DataType = 0
	// Tenum enum type
	Tenum = 1
	// Tstring string type
	Tstring = 2
)

// ColumnDescriptor describe a storage column
type ColumnDescriptor struct {
	Name          string
	JSONKey       string
	JSONMandatory bool
	EnumValues    []string
	NotNullable   bool
	Type          DataType
}

// Table contains columns
type Table struct {
	Name    string
	Columns []ColumnDescriptor
}

// UnmarshallJSONTableDescriptor unmarshall atable descriptor from a json file
func UnmarshallJSONTableDescriptor(path string) (*Table, error) {
	t := &Table{}
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return t, err
	}
	err = json.Unmarshal(content, t)
	if err != nil {
		return t, err
	}
	return t, nil
}
