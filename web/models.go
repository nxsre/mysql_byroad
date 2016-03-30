package web

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
	Fields         NotifyFields //任务订阅的字段
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
	Static         *Static
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

type NotifyFields []*NotifyField

type BinlogStatic struct {
	Schema string
	Table  string
	Event  string
	Count  uint64
}

type BinlogStatics struct {
	statics []*BinlogStatic
}

type Static struct {
	SendMessageCount   uint64
	ReSendMessageCount uint64
	SendSuccessCount   uint64
	SendFailedCount    uint64
}

type TaskStatic struct {
	statics map[int64]*Static
}

type LogList struct {
	Logs []string
	Host string
	Path string
}

type BinlogInfo struct {
	Filename string
	Position uint32
}
