package tablepacked

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
