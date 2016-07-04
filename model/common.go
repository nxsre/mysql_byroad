package model

import (
	"github.com/jmoiron/sqlx"
)

var confdb *sqlx.DB

// 不允许并发调用
func Init(db *sqlx.DB){
	confdb = db
}

type DataPackProtocal int
const(
	// 旁路系统默认数据封装格式: 消息内容从post的body中读取。
	PackProtocalDefault DataPackProtocal = iota
	// 使用消息中心的推送协议进行数据封装: message=POST["message"], jobid=GET["jobid"], retry_times=GET["retry_times"]
	PackProtocalEventCenter
)

type LogList struct {
	Logs []string
	Host string
	Path string
}

type BinlogInfo struct {
	Filename string
	Position uint32
}

type ColumnValue struct {
	ColunmName string      `json:"columnName"`
	Value      interface{} `json:"value"`
	OldValue   interface{} `json:"oldValue"`
}

type OrderedSchema struct {
	Schema        string
	OrderedTables []*OrderedTable
}

type OrderedTable struct {
	Table   string
	Columns []string
}

type OrderedTables []*OrderedTable

type OrderedSchemas []*OrderedSchema

func (o OrderedSchemas) Len() int {
	return len(o)
}

func (o OrderedSchemas) Less(i, j int) bool {
	return o[i].Schema < o[j].Schema
}

func (o OrderedSchemas) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
}

func (o OrderedTables) Len() int {
	return len(o)
}

func (o OrderedTables) Less(i, j int) bool {
	return o[i].Table < o[j].Table
}

func (o OrderedTables) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
}
