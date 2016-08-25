package main

import (
	"database/sql"
	"fmt"
	"mysql_byroad/model"
	"sort"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
)

type columnMap map[string]map[string][]string

type ColumnManager struct {
	username string
	password string
	host     string
	port     uint16
	exclude  []string
	db       *sql.DB
	columns  columnMap
	sync.RWMutex
}

/*
   读取mysql的information_schema表，获取所有列的相关信息
*/
func NewColumnManager(config MysqlInstanceConfig) *ColumnManager {
	cm := ColumnManager{
		username: config.Username,
		password: config.Password,
		host:     config.Host,
		port:     config.Port,
		exclude:  config.Exclude,
	}
	cm.getColumnsMap()
	return &cm
}

/*
	根据数据库名和表名，获取所有的字段
*/
func (this *ColumnManager) GetColumnNames(schema, table string) []string {
	cols := this.columns
	this.RLock()
	if cols[schema] != nil && cols[schema][table] != nil {
		names := cols[schema][table]
		this.RUnlock()
		return names
	} else {
		return this.UpdateGetColumnNames(schema, table)
	}
}

/*
	根据数据库名和表民，获取相应的index的字段名
*/
func (this *ColumnManager) GetColumnName(schema, table string, index int) string {
	colNames := this.GetColumnNames(schema, table)
	colLength := len(colNames)
	if index >= 0 && index < colLength {
		return colNames[index]
	} else {
		colNames = this.UpdateGetColumnNames(schema, table)
		colLength = len(colNames)
		if index >= 0 && index < colLength {
			return colNames[index]
		} else {
			return ""
		}
	}
}

/*
	根据数据库名和表名，更新其字段信息
*/
func (this *ColumnManager) UpdateGetColumnNames(schema, table string) []string {
	var err error
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/information_schema", this.username, this.password, this.host, this.port)
	this.db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Error("column manager: ", err.Error())
	}
	defer this.db.Close()
	stmt, err := this.db.Prepare("SELECT COLUMN_NAME FROM columns WHERE table_schema = ? AND table_name = ?")
	columnNames := []string{}
	if err != nil {
		log.Error("column manager: ", err.Error())
		return columnNames
	}
	defer stmt.Close()
	rows, err := stmt.Query(schema, table)
	if err != nil {
		log.Error("column manager: ", err.Error())
		return columnNames
	}
	for rows.Next() {
		var name string
		rows.Scan(&name)
		columnNames = append(columnNames, name)
	}
	this.Lock()
	if this.columns[schema] == nil {
		this.columns[schema] = make(map[string][]string, 100)
	}
	this.columns[schema][table] = columnNames
	this.Unlock()
	return columnNames
}

/*
	重新加载所有的数据库名，表名和字段信息
*/
func (this *ColumnManager) ReloadColumnsMap() {
	this.getColumnsMap()
}

func (this *ColumnManager) getColumnsMap() {
	columnsMap := make(columnMap)
	var err error
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/information_schema", this.username, this.password, this.host, this.port)
	this.db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Panic(err.Error())
		return
	}

	sqlStr := "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM columns "
	nodisplay := this.getNoDisplaySchema()
	if nodisplay != "" {
		sqlStr += "WHERE TABLE_SCHEMA NOT IN (" + nodisplay + ")"
	}
	stm, err := this.db.Prepare(sqlStr)
	if err != nil {
		log.Error("get columnsMap: ", err.Error())
		return
	}
	var rows *sql.Rows
	rows, err = stm.Query()
	if err != nil {
		log.Error("get columnsMap: ", err.Error())
		return
	}
	for rows.Next() {
		var tableSchema, tableName, columnName string
		rows.Scan(&tableSchema, &tableName, &columnName)
		if columnsMap[tableSchema] == nil {
			columnsMap[tableSchema] = make(map[string][]string, 100)
			columnsMap[tableSchema][tableName] = []string{}
		}
		columnsMap[tableSchema][tableName] = append(columnsMap[tableSchema][tableName], columnName)
	}
	this.Lock()
	this.columns = columnsMap
	this.Unlock()
	this.db.Close()
}

func getOrderedColumnsList(columns columnMap) model.OrderedSchemas {
	colslist := make(model.OrderedSchemas, 0, 10)
	for schema, tables := range columns {
		os := new(model.OrderedSchema)
		os.Schema = schema
		os.OrderedTables = make([]*model.OrderedTable, 0, 10)
		for table, columns := range tables {
			ot := new(model.OrderedTable)
			ot.Table = table
			ot.Columns = make([]string, 0, 10)
			for _, column := range columns {
				ot.Columns = append(ot.Columns, column)
			}
			os.OrderedTables = append(os.OrderedTables, ot)
		}
		colslist = append(colslist, os)
	}
	sort.Sort(colslist)
	for _, tab := range colslist {
		sort.Sort(model.OrderedTables(tab.OrderedTables))
	}
	return colslist
}

func (this *ColumnManager) GetOrderedColumns() model.OrderedSchemas {
	this.getColumnsMap()
	columns := this.columns
	this.RLock()
	defer this.RUnlock()
	return getOrderedColumnsList(columns)
}

func (this *ColumnManager) getNoDisplaySchema() string {
	var data string
	for _, schema := range this.exclude {
		data = data + "'" + schema + "'" + ","
	}
	return strings.TrimRight(data, ",")
}
