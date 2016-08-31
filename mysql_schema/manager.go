package schema

import (
	"strings"
	"sync"
)

type ColumnManager struct {
	inspectors []*Inspector
	sync.RWMutex
}

type ErrList []error

func (l ErrList) Error() string {
	var es []string
	for _, e := range l {
		es = append(es, e.Error())
	}
	return strings.Join(es, "\n")
}

func (l ErrList) Errors() []error {
	return l
}

func NewColumnManager(configs []*MysqlConfig) (*ColumnManager, error) {
	var errors []error
	cm := ColumnManager{
		inspectors: make([]*Inspector, 0, 10),
	}
	for _, config := range configs {
		insp, err := NewInspector(config)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		//insp.BuildColumnMap()
		cm.inspectors = append(cm.inspectors, insp)
	}
	if len(errors) != 0 {
		return &cm, ErrList(errors)
	}
	return &cm, nil
}

func (this *ColumnManager) LookupLoop() {
	for _, insp := range this.inspectors {
		go insp.LookupLoop()
	}
}

func (this *ColumnManager) BuildColumnMap() {
	for _, insp := range this.inspectors {
		insp.BuildColumnMap()
	}
}

func (this *ColumnManager) GetInspector(name string) *Inspector {
	for _, inspector := range this.inspectors {
		if inspector.config.Name == name {
			return inspector
		}
	}
	return nil
}

func (this *ColumnManager) Inspectors() []*Inspector {
	return this.inspectors
}

func (this *ColumnManager) InspectorNames() []string {
	names := []string{}
	for _, inspector := range this.inspectors {
		names = append(names, inspector.config.Name)
	}
	return names
}

func (this *ColumnManager) GetColumns(schema, table string) ColumnList {
	for _, inspector := range this.inspectors {
		cl := inspector.GetColumnMap().Columns(schema, table)
		if cl != nil {
			return cl
		}
	}
	return nil
}

func (this *ColumnManager) GetColumn(schema, table string, index int) *Column {
	for _, inspector := range this.inspectors {
		if column := inspector.GetColumnMap().GetColumn(schema, table, index); column != nil {
			return column
		}
	}
	return nil
}

func (this *ColumnManager) Close() error {
	var errors []error
	for _, inspector := range this.inspectors {
		err := inspector.Close()
		if err != nil {
			errors = append(errors, err)
		}
	}
	return ErrList(errors)
}
