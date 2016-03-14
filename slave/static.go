package slave

import (
	"sync"
	"sync/atomic"

	"github.com/jmoiron/sqlx"
)

type Static struct {
	SendMessageCount   uint64
	ReSendMessageCount uint64
	SendSuccessCount   uint64
	SendFailedCount    uint64
}

func (this *Static) IncSendMessageCount() {
	atomic.AddUint64(&this.SendMessageCount, 1)
}

func (this *Static) IncReSendMessageCount() {
	atomic.AddUint64(&this.ReSendMessageCount, 1)
}

func (this *Static) IncSendSuccessCount() {
	atomic.AddUint64(&this.SendSuccessCount, 1)
}

func (this *Static) IncSendFailedCount() {
	atomic.AddUint64(&this.SendFailedCount, 1)
}

type TaskStatic struct {
	statics map[int64]*Static
	wg      sync.WaitGroup
	ch      chan bool
}

func createStaticTable(confdb *sqlx.DB) {
	s := "CREATE TABLE IF NOT EXISTS `static`(" +
		"`id` INTEGER PRIMARY KEY AUTOINCREMENT," +
		"`task_id` INTEGER NOT NULL," +
		"`send_message_count` INTEGER," +
		"`resend_message_count` INTEGER," +
		"`send_success_count` INTEGER," +
		"`send_failed_count` INTEGER" +
		")"
	confdb.MustExec(s)
	s = "CREATE TABLE IF NOT EXISTS `binlog_static`(" +
		"`id` INTEGER PRIMARY KEY AUTOINCREMENT," +
		"`schema` VARCHAR(255) NOT NULL," +
		"`table` VARCHAR(255) NOT NULL," +
		"`count` INTEGER" +
		")"
	confdb.MustExec(s)
}

func NewTaskStatic() *TaskStatic {
	return &TaskStatic{
		statics: make(map[int64]*Static),
		ch:      make(chan bool, 1),
	}
}

func (this *TaskStatic) Save(confdb *sqlx.DB) {
	for taskid, static := range this.statics {
		var cnt int64
		err := confdb.Get(&cnt, "SELECT COUNT(*) FROM static WHERE `task_id`=?", taskid)
		sysLogger.LogErr(err)
		if err != nil {
			return
		}
		if cnt == 0 {
			_, err = confdb.Exec("INSERT INTO static(task_id, send_message_count, resend_message_count, send_success_count, send_failed_count) VALUES(?,?,?,?,?)", taskid, static.SendMessageCount, static.ReSendMessageCount, static.SendSuccessCount, static.SendFailedCount)
			sysLogger.LogErr(err)
		} else {
			_, err = confdb.Exec("UPDATE static SET send_message_count=?, resend_message_count=?, send_success_count=?, send_failed_count=? WHERE task_id=?", static.SendMessageCount, static.ReSendMessageCount, static.SendSuccessCount, static.SendFailedCount, taskid)
			sysLogger.LogErr(err)
		}
	}
}

func (this *TaskStatic) Init(confdb *sqlx.DB) {
	s := "SELECT task_id, send_message_count, resend_message_count, send_success_count, send_failed_count FROM static"
	rows, err := confdb.Query(s)
	if err != nil {
		sysLogger.LogErr(err)
		return
	}
	for rows.Next() {
		static := Static{}
		var id int64
		err := rows.Scan(&id, &static.SendMessageCount, &static.ReSendMessageCount, &static.SendSuccessCount, &static.SendFailedCount)
		if err != nil {
			sysLogger.LogErr(err)
			return
		}
		this.statics[id] = &static
	}
	if ts, ok := this.statics[0]; ok {
		totalStatic = *ts
	}
	this.statics[0] = &totalStatic
}

func (this *TaskStatic) Tick(arg interface{}) {
	confdb := arg.(*sqlx.DB)
	this.Save(confdb)
}

func (this *TaskStatic) IncSendMessageCount(taskid int64) {
	static, ok := this.statics[taskid]
	if !ok {
		static = &Static{}
		this.statics[taskid] = static
	}
	static.IncSendMessageCount()
}

func (this *TaskStatic) IncReSendMessageCount(taskid int64) {
	static, ok := this.statics[taskid]
	if !ok {
		static = &Static{}
		this.statics[taskid] = static
	}
	static.IncReSendMessageCount()
}

func (this *TaskStatic) IncSendSuccessCount(taskid int64) {
	static, ok := this.statics[taskid]
	if !ok {
		static = &Static{}
		this.statics[taskid] = static
	}
	static.IncSendSuccessCount()
}

func (this *TaskStatic) IncSendFailedCount(taskid int64) {
	static, ok := this.statics[taskid]
	if !ok {
		static = &Static{}
		this.statics[taskid] = static
	}
	static.IncSendFailedCount()
}

func (this *TaskStatic) Get(taskid int64) *Static {
	return this.statics[taskid]
}

type BinlogStatic struct {
	Schema string
	Table  string
	Event  string
	Count  uint64
}

type BinlogStatics struct {
	Statics []*BinlogStatic
}

func (this *BinlogStatics) IncStatic(schema, table, event string) {
	for _, si := range this.Statics {
		if si.Schema == schema && si.Table == table && si.Event == event {
			atomic.AddUint64(&si.Count, 1)
			return
		}
	}
	info := &BinlogStatic{schema, table, event, 1}
	this.Statics = append(this.Statics, info)
}

func (this *BinlogStatics) Save(confdb *sqlx.DB) {

}

func (this *BinlogStatics) Tick(arg interface{}) {
	confdb := arg.(*sqlx.DB)
	this.Save(confdb)
}
