package schema

import (
	"testing"
)

func TestGetEnumValue(t *testing.T) {
	column := Column{
		Schema:     "test",
		Table:      "user",
		Name:       "habby",
		DataType:   "enum",
		ColumnType: "ENUM('one', 'tow')",
	}
	if column.IsEnum() {
		e1 := column.GetEnumValue(0)
		t.Log(e1)
		if e1 != "one" {
			t.Errorf("enum value %s not equal %s\n", e1, "one")
		}
		e2 := column.GetEnumValue(1)
		t.Log(e2)
		if e2 != "tow" {
			t.Errorf("enum value %s not equal %s\n", e2, "tow")
		}
		e3 := column.GetEnumValue(-1)
		t.Log(e3)
		if e3 != "" {
			t.Errorf("enum value %s not equal %s\n", e3, "")
		}
		e4 := column.GetEnumValue(4)
		t.Log(e4)
		if e4 != "" {
			t.Errorf("enum value %s not equal %s\n", e4, "")
		}
	}
}
