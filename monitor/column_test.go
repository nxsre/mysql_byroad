package main

import "testing"

func TestGetSchemas(t *testing.T) {
	config := MysqlInstanceConfig{
		Host:     "127.0.0.1",
		Port:     3306,
		Username: "root",
		Password: "toor",
		Exclude:  []string{"information_schema", "performance_schema", "mysql"},
	}
	columnManager, err := NewColumnManager(config)
	if err != nil {
		t.Error(err)
	}
	schemas, err := columnManager.GetSchemas()
	if err != nil {
		t.Error(err)
	}
	t.Logf("schemas: %v", schemas)
	columnManager.Close()
}

func TestGetTables(t *testing.T) {
	config := MysqlInstanceConfig{
		Host:     "127.0.0.1",
		Port:     3306,
		Username: "root",
		Password: "toor",
		Exclude:  []string{"information_schema", "performance_schema", "mysql"},
	}
	columnManager, err := NewColumnManager(config)
	if err != nil {
		t.Error(err)
	}
	tables, err := columnManager.GetTables("byroad")
	if err != nil {
		t.Error(err)
	}
	t.Logf("tables: %v", tables)
	columnManager.Close()
}

func TestGetColumns(t *testing.T) {
	config := MysqlInstanceConfig{
		Host:     "127.0.0.1",
		Port:     3306,
		Username: "root",
		Password: "toor",
		Exclude:  []string{"information_schema", "performance_schema", "mysql"},
	}
	columnManager, err := NewColumnManager(config)
	if err != nil {
		t.Error(err)
	}
	columns, err := columnManager.GetColumns("byroad", "task")
	if err != nil {
		t.Error(err)
	}
	t.Logf("columns: %v", columns)
	columnManager.Close()
}
