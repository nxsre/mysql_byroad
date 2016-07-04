package model

import (
	"sync"
	"sync/atomic"
	"github.com/juju/errors"
	"database/sql"
)

type Statistic struct {
	SendMessageCount   uint64
	ReSendMessageCount uint64
	SendSuccessCount   uint64
	SendFailedCount    uint64
}

func CreateStatisticTable() {
	s := "CREATE TABLE IF NOT EXISTS `statistic`(" +
	"`id` INTEGER PRIMARY KEY AUTOINCREMENT," +
	"`task_id` INTEGER NOT NULL," +
	"`send_message_count` INTEGER," +
	"`resend_message_count` INTEGER," +
	"`send_success_count` INTEGER," +
	"`send_failed_count` INTEGER" +
	")"
	confdb.MustExec(s)
	s = "CREATE TABLE IF NOT EXISTS `binlog_statistic`(" +
	"`id` INTEGER PRIMARY KEY AUTOINCREMENT," +
	"`schema` VARCHAR(255) NOT NULL," +
	"`table` VARCHAR(255) NOT NULL," +
	"`count` INTEGER" +
	")"
	confdb.MustExec(s)
}

func (this *Statistic) IncSendMessageCount() {
	atomic.AddUint64(&this.SendMessageCount, 1)
}

func (this *Statistic) IncReSendMessageCount() {
	atomic.AddUint64(&this.ReSendMessageCount, 1)
}

func (this *Statistic) IncSendSuccessCount() {
	atomic.AddUint64(&this.SendSuccessCount, 1)
}

func (this *Statistic) IncSendFailedCount() {
	atomic.AddUint64(&this.SendFailedCount, 1)
}

type TaskStatistics struct {
	statistics map[int64]*Statistic
	wg      sync.WaitGroup
	ch      chan bool
}

func NewTaskStatistics() *TaskStatistics {
	return &TaskStatistics{
		statistics: make(map[int64]*Statistic),
		ch:      make(chan bool, 1),
	}
}

func (this *TaskStatistics) Save()(err error){
	errs := []error{}
	for taskid, statistic := range this.statistics {
		var cnt int64
		err = confdb.Get(&cnt, "SELECT COUNT(*) FROM statistic WHERE `task_id`=?", taskid)
		if err != nil {
			return
		}
		if cnt == 0 {
			_, err = confdb.Exec("INSERT INTO statistic(task_id, send_message_count, resend_message_count, send_success_count, send_failed_count) VALUES(?,?,?,?,?)", taskid, statistic.SendMessageCount, statistic.ReSendMessageCount, statistic.SendSuccessCount, statistic.SendFailedCount)
		} else {
			_, err = confdb.Exec("UPDATE statistic SET send_message_count=?, resend_message_count=?, send_success_count=?, send_failed_count=? WHERE task_id=?", statistic.SendMessageCount, statistic.ReSendMessageCount, statistic.SendSuccessCount, statistic.SendFailedCount, taskid)
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0{
		errStr := ""
		for _, e := range errs{
			errStr += "[" + e.Error() + "] "
		}
		err = errors.New(errStr)
	}
	return
}

func (this *TaskStatistics) Init()(err error) {
	s := "SELECT task_id, send_message_count, resend_message_count, send_success_count, send_failed_count FROM statistic"
	var rows *sql.Rows
	rows, err = confdb.Query(s)
	if err != nil {
		return
	}
	for rows.Next() {
		statistic := Statistic{}
		var id int64
		err = rows.Scan(&id, &statistic.SendMessageCount, &statistic.ReSendMessageCount, &statistic.SendSuccessCount, &statistic.SendFailedCount)
		if err != nil {
			return
		}
		this.statistics[id] = &statistic
	}
	return
}

func (this *TaskStatistics) Tick(_ interface{}) {
	this.Save()
}

func (this *TaskStatistics) IncSendMessageCount(taskid int64) {
	statistic, ok := this.statistics[taskid]
	if !ok {
		statistic = &Statistic{}
		this.statistics[taskid] = statistic
	}
	statistic.IncSendMessageCount()
}

func (this *TaskStatistics) IncReSendMessageCount(taskid int64) {
	statistic, ok := this.statistics[taskid]
	if !ok {
		statistic = &Statistic{}
		this.statistics[taskid] = statistic
	}
	statistic.IncReSendMessageCount()
}

func (this *TaskStatistics) IncSendSuccessCount(taskid int64) {
	statistic, ok := this.statistics[taskid]
	if !ok {
		statistic = &Statistic{}
		this.statistics[taskid] = statistic
	}
	statistic.IncSendSuccessCount()
}

func (this *TaskStatistics) IncSendFailedCount(taskid int64) {
	statistic, ok := this.statistics[taskid]
	if !ok {
		statistic = &Statistic{}
		this.statistics[taskid] = statistic
	}
	statistic.IncSendFailedCount()
}

func (this *TaskStatistics) Get(taskid int64) *Statistic {
	return this.statistics[taskid]
}

