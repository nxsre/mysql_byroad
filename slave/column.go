package slave

import (
	"database/sql"
	"fmt"
	"mysql_byroad/common"
	"sort"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

type columnMap map[string]map[string][]string

type ColumnManager struct {
	username string
	password string
	host     string
	port     int
	db       *sql.DB
	columns  columnMap
	sync.RWMutex
}

/*
   读取mysql的information_schema表，获取所有列的相关信息
*/
func NewColumnManager(config *common.MysqlConfig) *ColumnManager {
	cm := ColumnManager{
		username: config.Username,
		password: config.Password,
		host:     config.Host,
		port:     config.Port,
	}
	cm.getColumnsMap()
	return &cm
}

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

func (this *ColumnManager) UpdateGetColumnNames(schema, table string) []string {
	var err error
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/information_schema", this.username, this.password, this.host, this.port)
	this.db, err = sql.Open("mysql", dsn)
	sysLogger.LogErr(err)
	defer this.db.Close()
	stmt, err := this.db.Prepare("SELECT COLUMN_NAME FROM columns WHERE table_schema = ? AND table_name = ?")
	sysLogger.LogErr(err)
	columnNames := []string{}
	if err != nil {
		return columnNames
	}
	defer stmt.Close()
	rows, err := stmt.Query(schema, table)
	sysLogger.LogErr(err)
	if err != nil {
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

func (this *ColumnManager) ReloadColumnsMap() {
	this.getColumnsMap()
}

func (this *ColumnManager) getColumnsMap() {
	columnsMap := make(columnMap)
	var err error
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/information_schema", this.username, this.password, this.host, this.port)
	this.db, err = sql.Open("mysql", dsn)
	sysLogger.LogErr(err)
	nodisplay := getNoDisplaySchema()
	rows, err := this.db.Query("SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM columns WHERE TABLE_SCHEMA NOT IN (" + nodisplay + ")")
	sysLogger.LogErr(err)
	if err != nil {
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

func getOrderedColumnsList(columns columnMap) common.OrderedSchemas {
	colslist := make(common.OrderedSchemas, 0, 10)
	for schema, tables := range columns {
		os := new(common.OrderedSchema)
		os.Schema = schema
		os.OrderedTables = make([]*common.OrderedTable, 0, 10)
		for table, columns := range tables {
			ot := new(common.OrderedTable)
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
		sort.Sort(common.OrderedTables(tab.OrderedTables))
	}
	return colslist
}

func (this *ColumnManager) GetOrderedColumns() common.OrderedSchemas {
	columns := this.columns
	this.RLock()
	defer this.RUnlock()
	return getOrderedColumnsList(columns)
}

func getNoDisplaySchema() string {
	schemas := configer.GetArray("mysql", "nodisplay", " ")
	var data string
	for _, schema := range schemas {
		data = data + "'" + schema + "'" + ","
	}
	return strings.TrimRight(data, ",")
}
