package schema

import (
	"testing"
	"time"
)

var inspector *Inspector

func init() {
	config := MysqlConfig{
		Host:     "127.0.0.1",
		Port:     3306,
		Username: "root",
		Password: "toor",
		Exclude:  []string{"information_schema", "performance_schema", "mysql"},
		Interval: time.Second,
	}
	var err error
	inspector, err = NewInspector(&config)
	if err != nil {
		panic(err)
	}
}

func TestInspector(t *testing.T) {
	cl, err := inspector.getColumns()
	if err != nil {
		t.Error(err)
	}
	if len(cl) == 0 {
		t.Error("columns length = 0")
	}
	inspector.buildColumnMap(cl)
	names := inspector.GetColumnMap().ColumNames("byroad", "task")
	t.Logf("column map get column names: %+v", names)
	if len(names) == 0 {
		t.Error("column names length == 0")
	}
}

func TestGetSchemas(t *testing.T) {
	schemas, err := inspector.GetSchemas()
	if err != nil {
		t.Error(err)
	}
	if len(schemas) == 0 {
		t.Error("get schemas length equals 0")
	}
	t.Logf("schemas: %v", schemas)
}

func TestGetTables(t *testing.T) {
	tables, err := inspector.GetTables("byroad")
	if err != nil {
		t.Error(err)
	}

	if len(tables) == 0 {
		t.Error("get byroad tables length equals 0")
	}
	t.Logf("byroad tables: %v", tables)

	tables, err = inspector.GetTables("helloworld")
	if err != nil {
		t.Error(err)
	}
	if len(tables) != 0 {
		t.Error("get helloworld tables length not equals 0")
	}
}

func TestGetColumns(t *testing.T) {
	columns, err := inspector.GetColumns("byroad", "task")
	if err != nil {
		t.Error(err)
	}
	if len(columns) == 0 {
		t.Error("get byroad.task columns length equals 0")
	}
	t.Logf("byroad.task columns%v", columns)

	columns, err = inspector.GetColumns("byroad", "hello_world")
	if err != nil {
		t.Error(err)
	}
	if len(columns) != 0 {
		t.Error("get byroad.hello_world columns length not equals 0")
	}

	columns, err = inspector.GetColumns("hello", "world")
	if err != nil {
		t.Error(err)
	}
	if len(columns) != 0 {
		t.Error("get hello.world columns length not equals 0")
	}
}
