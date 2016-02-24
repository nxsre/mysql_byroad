package common

import "time"

/*
任务model
*/
type Task struct {
	ID             int64
	Name           string
	Apiurl         string //推送的url
	Event          string
	Stat           string
	Fields         []*NotifyField //任务订阅的字段
	CreateTime     time.Time
	CreateUser     string
	RoutineCount   int //推送协程数
	ReRoutineCount int //重推协程数
	ReSendTime     int //重推时间间隔
	RetryCount     int //重推次数
	Timeout        int //消息处理超时
	QueueLength    int64
	ReQueueLength  int64
	Desc           string
}

/*
数据库-表-字段对应任务的一个订阅
*/
type NotifyField struct {
	ID         int64
	Schema     string
	Table      string
	Column     string
	Send       int
	TaskID     int64
	CreateTime time.Time
}

/*
配置信息
*/
type Config struct {
	ID          int64
	Key         string
	Value       string
	Description string
}

/*
   对应一个推送的消息对象
*/
type NotifyEvent struct {
	Event        string         `json:"event"`
	Schema       string         `json:"schema"`
	Table        string         `json:"table"`
	Fields       []*ColumnValue `json:"fields"`
	Keys         []string       `json:"keys"`
	RetryCount   int            `json:"retryCount"`
	LastSendTime time.Time      `json:"lastSendTime"`
	TaskID       int64          `json:"taskID"`
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
