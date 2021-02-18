package tablepacked

import (
	"fmt"
	"reflect"
)

// StructToRow take a struct and fill the row data equivalent
func StructToRow(s interface{}, row *RowData, table Table) error {
	v := reflect.ValueOf(s)
	k := v.Kind()
	switch k {
	case reflect.Interface, reflect.Ptr:
		v = v.Elem()
	}

	k = v.Kind()

	if k != reflect.Struct {
		return fmt.Errorf("could not extract struct data from: %s", k.String())
	}

	if cap(row.Data) < len(table.Columns) {
		row.Data = make([]ColumnData, len(table.Columns))
	} else {
		row.Data = row.Data[0:len(table.Columns)]
	}

	typeOfS := v.Type()
	for ic, c := range table.Columns {
		fieldType, found := typeOfS.FieldByName(c.Name)
		if !found {
			if c.JSONMandatory {
				return fmt.Errorf("field '%s' mandatory is not in the structure", c.Name)
			}
			continue
		}
		fieldKind := fieldType.Type.Kind()
		switch c.Type {
		case Tuint:
			switch fieldKind {
			case
				reflect.Uint,
				reflect.Uint8,
				reflect.Uint16,
				reflect.Uint32,
				reflect.Uint64:
				fieldValue := v.FieldByName(c.Name)
				//if
				row.Data[ic].EncodedRawValue = fieldValue.Uint()
			case reflect.Int,
				reflect.Int8,
				reflect.Int16,
				reflect.Int32,
				reflect.Int64:
				fieldValue := v.FieldByName(c.Name)
				intVal := fieldValue.Int()
				if intVal < 0 {
					return fmt.Errorf("could not set %s with negative value: %d", c.Name, intVal)
				}
				row.Data[ic].EncodedRawValue = uint64(intVal)
			default:
				return fmt.Errorf("could not set %s with field kind: %s", c.Name, fieldKind.String())
			}

		case Tenum:
			switch fieldKind {
			case
				reflect.Int,
				reflect.Int8,
				reflect.Int16,
				reflect.Int32,
				reflect.Int64:
				fieldValue := v.FieldByName(c.Name)
				intValue := fieldValue.Int()
				if intValue < 0 {
					return fmt.Errorf("could not get negative enum values %d for field '%s'", intValue, c.Name)
				}
				row.Data[ic].EncodedRawValue = uint64(intValue)
			case
				reflect.Uint,
				reflect.Uint8,
				reflect.Uint16,
				reflect.Uint32,
				reflect.Uint64:
				fieldValue := v.FieldByName(c.Name)
				row.Data[ic].EncodedRawValue = fieldValue.Uint()
			case reflect.String:
				fieldValue := v.FieldByName(c.Name)
				stringVal := fieldValue.String()
				encodedRawValue := -1
				for ie, ev := range c.EnumValues {
					if ev == stringVal {
						encodedRawValue = ie
						break
					}
				}
				if encodedRawValue < 0 {
					return fmt.Errorf("could not get enum values '%s' for field '%s'", stringVal, c.Name)
				}
				row.Data[ic].EncodedRawValue = uint64(encodedRawValue)
			default:
				return fmt.Errorf("could not set %s with field kind: %s", c.Name, fieldKind.String())
			}
		case Tstring:
			switch fieldKind {
			case reflect.String:
				fieldValue := v.FieldByName(c.Name)
				stringVal := fieldValue.String()
				row.Data[ic].EncodedRawValue = uint64(len(stringVal))
				row.Data[ic].Buffer = []byte(stringVal)
			}
		default:
			return fmt.Errorf("undefined column %s type: %d", c.Name, c.Type)
		}
	}
	return nil
}

// RowToStruct take a row and fill the struct with info
func RowToStruct(dst interface{}, row *RowData, table Table) error {
	v := reflect.ValueOf(dst)
	k := v.Kind()
	switch k {
	case reflect.Interface, reflect.Ptr:
		v = v.Elem()
	default:
		return fmt.Errorf("could not set value on %s", k.String())
	}
	k = v.Kind()

	typeOfS := v.Type()
	for ic, rc := range row.Data {
		c := table.Columns[ic]
		fieldType, found := typeOfS.FieldByName(c.Name)
		if !found {
			if c.JSONMandatory {
				return fmt.Errorf("field '%s' mandatory is not in the structure", c.Name)
			}
			continue
		}
		fieldKind := fieldType.Type.Kind()
		switch c.Type {
		case Tuint:
			switch fieldKind {
			case
				reflect.Int,
				reflect.Int8,
				reflect.Int16,
				reflect.Int32,
				reflect.Int64:

				fieldValue := v.FieldByName(c.Name)
				fieldValue.SetInt(int64(rc.EncodedRawValue))
			case
				reflect.Uint,
				reflect.Uint8,
				reflect.Uint16,
				reflect.Uint32,
				reflect.Uint64:

				fieldValue := v.FieldByName(c.Name)
				fieldValue.SetUint(rc.EncodedRawValue)
			default:
				return fmt.Errorf("could not set %s with field kind: %s", c.Name, fieldKind.String())
			}

		case Tenum:
			switch fieldKind {
			case
				reflect.Uint,
				reflect.Uint8,
				reflect.Uint16,
				reflect.Uint32,
				reflect.Uint64,
				reflect.Int,
				reflect.Int8,
				reflect.Int16,
				reflect.Int32,
				reflect.Int64:
				fieldValue := v.FieldByName(c.Name)
				fieldValue.SetInt(int64(rc.EncodedRawValue))
			case reflect.String:
				fieldValue := v.FieldByName(c.Name)
				fieldValue.SetString(c.EnumValues[int(rc.EncodedRawValue)])
			default:
				return fmt.Errorf("could not set %s with field kind: %s", c.Name, fieldKind.String())
			}
		case Tstring:
			switch fieldKind {
			case reflect.String:
				fieldValue := v.FieldByName(c.Name)
				fieldValue.SetString(string(rc.Buffer))
			}
		default:
			return fmt.Errorf("undefined column %s type: %d", c.Name, c.Type)
		}
	}
	return nil
}
