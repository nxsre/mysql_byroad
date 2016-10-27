package schema

import (
	"regexp"
	"sync"
)

type Column struct {
	Schema     string `db:"TABLE_SCHEMA"`
	Table      string `db:"TABLE_NAME"`
	Name       string `db:"COLUMN_NAME"`
	DataType   string `db:"DATA_TYPE"`
	ColumnType string `db:"COLUMN_TYPE"`
}

type ColumnList []*Column

type ColumnMap struct {
	columns map[string]map[string]ColumnList
	sync.RWMutex
}

func BuildColumnMap(columns ColumnList) *ColumnMap {
	cs := make(map[string]map[string]ColumnList, 100)
	cm := ColumnMap{
		columns: cs,
	}
	for _, column := range columns {
		_, ok := cs[column.Schema]
		if !ok {
			cs[column.Schema] = make(map[string]ColumnList)
		}
		_, ok = cs[column.Schema]
		if !ok {
			cs[column.Schema][column.Table] = make([]*Column, 0, 10)
		}
		cs[column.Schema][column.Table] = append(cs[column.Schema][column.Table], column)
	}
	return &cm
}

func (this *ColumnMap) GetColumns() map[string]map[string]ColumnList {
	this.RLock()
	defer this.RUnlock()
	return this.columns
}

func (this *ColumnMap) Schemas() []string {
	schemas := make([]string, 0, 10)
	this.RLock()
	defer this.RUnlock()
	for schema, _ := range this.columns {
		schemas = append(schemas, schema)
	}
	return schemas
}

func (this *ColumnMap) Tables(schema string) []string {
	this.RLock()
	defer this.RUnlock()
	tableMap, ok := this.columns[schema]
	if !ok {
		return nil
	}
	tables := make([]string, 0, 10)
	for table, _ := range tableMap {
		tables = append(tables, table)
	}
	return tables
}

func (this *ColumnMap) Columns(schema, table string) ColumnList {
	this.RLock()
	defer this.RUnlock()
	if tableMap, ok := this.columns[schema]; ok {
		return tableMap[table]
	}
	return nil
}

func (this *ColumnMap) ColumNames(schema, table string) []string {
	this.RLock()
	defer this.RUnlock()
	columns := this.Columns(schema, table)
	if columns == nil {
		return nil
	}
	names := make([]string, 0, 10)
	for _, column := range columns {
		names = append(names, column.Name)
	}
	return names
}

func (this *ColumnMap) GetColumn(schema, table string, index int) *Column {
	this.RLock()
	defer this.RUnlock()
	columns := this.Columns(schema, table)
	if columns != nil && index >= 0 && index < len(columns) {
		return columns[index]
	}
	return nil
}

func (this *ColumnMap) GetColumnByName(schema, table, column string) *Column {
	this.RLock()
	defer this.RUnlock()
	columns := this.Columns(schema, table)
	for _, c := range columns {
		if column == c.Name {
			return c
		}
	}
	return nil
}

func (this *Column) IsUnsigned() bool {
	return StringContainsIgnoreCase(this.DataType, "unsigned")
}

func (this *Column) IsEnum() bool {
	return StringContainsIgnoreCase(this.DataType, "enum")
}

func (this *Column) IsSet() bool {
	return StringContainsIgnoreCase(this.DataType, "set")
}

func (this *Column) IsText() bool {
	return StringContainsIgnoreCase(this.DataType, "text")
}

func (this *Column) GetEnumValue(index int) string {
	values := this.getEnumValues()
	if index >= 0 && index < len(values) {
		return values[index]
	}
	return ""
}

func (this *Column) getEnumValues() []string {
	s := `'(\w+)'`
	re := regexp.MustCompile(s)
	matches := re.FindAllStringSubmatch(this.ColumnType, -1)
	values := make([]string, 0, 5)
	for _, m := range matches {
		values = append(values, m[1])
	}
	return values
}
