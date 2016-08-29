package mysql

import (
	"testing"
	"time"
)

func TestInspector(t *testing.T) {
	config := MysqlConfig{
		Host:     "127.0.0.1",
		Port:     3306,
		Username: "root",
		Password: "root",
		Exclude:  []string{"information_schema", "performance_schema", "mysql"},
		Interval: time.Second,
	}

	inspector, err := NewInspector(&config)
	if err != nil {
		t.Error(err)
	}
	cl, err := inspector.getColumns()
	if err != nil {
		t.Error(err)
	}
	if len(cl) == 0 {
		t.Error("columns length = 0")
	}
	inspector.buildColumnMap(cl)
	names := inspector.GetColumnMap().ColumNames("byroad", "task")
	t.Logf("%+v", names)
	if len(names) == 0 {
		t.Error("column names length == 0")
	}
	go inspector.InspectLoop()
	time.Sleep(10 * time.Second)
}
