package slave

import (
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
}

func NewTaskStatic() *TaskStatic {
	return &TaskStatic{
		statics: make(map[int64]*Static),
	}
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
