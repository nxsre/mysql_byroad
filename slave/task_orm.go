package slave

import (
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

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

func createTaskTable(confdb *sqlx.DB) {
	s := "CREATE TABLE IF NOT EXISTS `task` (" +
		"`id` INTEGER PRIMARY KEY AUTOINCREMENT," +
		"`name` VARCHAR(120) NOT NULL," +
		"`apiurl` VARCHAR(120) NOT NULL," +
		"`event` VARCHAR(120) NOT NULL," +
		"`stat` VARCHAR(120) NOT NULL," +
		"`create_time` DATE NOT NULL," +
		"`create_user` VARCHAR(120) NOT NULL," +
		"`routine_count` INTEGER NOT NULL," +
		"`re_routine_count` INTEGER NOT NULL," +
		"`re_send_time` INTEGER NOT NULL," +
		"`retry_count` INTEGER NOT NULL," +
		"`timeout` INTEGER NOT NULL," +
		"`desc` VARCHAR(255)" +
		")"
	confdb.MustExec(s)
}

func createNotifyFieldTable(confdb *sqlx.DB) {
	s := "CREATE TABLE IF NOT EXISTS `notify_field`(" +
		"`id` INTEGER PRIMARY KEY AUTOINCREMENT," +
		"`schema` VARCHAR(120) NOT NULL," +
		"`table` VARCHAR(120) NOT NULL," +
		"`column` VARCHAR(120) NOT NULL," +
		"`send` INTERGE NOT NULL," +
		"`task_id` INTEGER NOT NULL," +
		"`create_time` DATE NOT NULL" +
		")"
	confdb.Exec(s)
}

func (task *Task) _insert(confdb *sqlx.DB) (id int64, err error) {
	s := "INSERT INTO `task`(`name`, `apiurl`, `event`, `stat`, `create_time`, `create_user`, `routine_count`, `re_routine_count`, `re_send_time`, `retry_count`, `timeout`, `desc`) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
	res, err := confdb.Exec(s, task.Name, task.Apiurl, task.Event, task.Stat, task.CreateTime, task.CreateUser, task.RoutineCount, task.ReRoutineCount, task.ReSendTime, task.RetryCount, task.Timeout, task.Desc)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (task *Task) _getByID(confdb *sqlx.DB) (*Task, error) {
	fields := make([]*NotifyField, 0)
	s := "SELECT * FROM `task` WHERE `id`=?"
	err := confdb.Get(task, s, task.ID)
	if err != nil {
		return nil, err
	}
	s = "SELECT * FROM `notify_field` WHERE `task_id`=?"
	rows, err := confdb.Queryx(s, task.ID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		f := new(NotifyField)
		rows.StructScan(f)
		f.TaskID = task.ID
		fields = append(fields, f)
	}
	task.Fields = fields
	return task, nil
}

func (task *Task) _update(confdb *sqlx.DB) (int64, error) {
	task.Fields._delete(task.ID, confdb)
	s := "UPDATE `task` SET `apiurl`=?, `event`=?, `name`=?, `stat`=?, `create_time`=?, `routine_count`=?, `re_routine_count`=?, `re_send_time`=?, `retry_count`=?, `timeout`=?, `desc`=? WHERE `id`=?"
	res, err := confdb.Exec(s, task.Apiurl, task.Event, task.Name, task.Stat, task.CreateTime, task.RoutineCount, task.ReRoutineCount, task.ReSendTime, task.RetryCount, task.Timeout, task.Desc, task.ID)
	if err != nil {
		return 0, err
	}
	task.Fields._insert(task.ID, confdb)
	return res.RowsAffected()
}

//delete task and its fields
func (task *Task) _delete(confdb *sqlx.DB) (int64, error) {
	s := "DELETE FROM `task` WHERE `id`=?"
	res, err := confdb.Exec(s, task.ID)
	if err != nil {
		return 0, err
	}
	s = "DELETE FROM `notify_field` WHERE `task_id`=?"
	res, err = confdb.Exec(s, task.ID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (field *NotifyField) _insert(confdb *sqlx.DB) (id int64, err error) {
	s := "INSERT INTO `notify_field`(`schema`, `table`, `column`, `send`, `task_id`,`create_time`) VALUES(?, ?, ?, ?, ?, ?)"
	stmt, err := confdb.Prepare(s)
	defer stmt.Close()
	if err != nil {
		return 0, err
	}
	res, err := stmt.Exec(field.Schema, field.Table, field.Column, field.Send, field.TaskID, time.Now())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (fields NotifyFields) _insert(taskID int64, confdb *sqlx.DB) error {
	if len(fields) == 0 {
		return nil
	}
	s := "INSERT INTO `notify_field`(`schema`, `table`, `column`, `send`, `task_id`,`create_time`) VALUES"
	fs := []interface{}{}
	for _, f := range fields {
		f.TaskID = taskID
		s += "(?, ?, ?, ?, ?, ?),"
		fs = append(fs, f.Schema, f.Table, f.Column, f.Send, f.TaskID, time.Now())
	}
	s = strings.TrimRight(s, ",")
	stmt, err := confdb.Prepare(s)
	if err != nil {
		return err
	}
	res, err := stmt.Exec(fs...)
	if err != nil {
		return err
	}
	_, err = res.RowsAffected()
	return err
}

func (field *NotifyField) _delete(confdb *sqlx.DB) (int64, error) {
	s := "DELETE FROM `notify_field` WHERE `id`=?"
	res, err := confdb.Exec(s, field.ID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (fields NotifyFields) _delete(taskid int64, confdb *sqlx.DB) (int64, error) {
	s := "DELETE FROM `notify_field` WHERE `task_id`=?"
	res, err := confdb.Exec(s, taskid)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

/*
读取数据库中的task和field，将其放入内存的taskMap中
*/
func _selectAllTasks(confdb *sqlx.DB) *TaskIdMap {
	tasks := NewTaskIdMap(100)
	s := "SELECT `id`, `name`, `apiurl`, `event`, `stat`, `create_time`, `create_user`,`routine_count`, `re_routine_count`, `re_send_time`, `retry_count`, `timeout`, `desc` FROM `task`"
	stmt, err := confdb.Prepare(s)
	defer stmt.Close()
	sysLogger.LogErr(err)
	if err != nil {
		return tasks
	}
	rows, err := stmt.Query()
	sysLogger.LogErr(err)
	if err != nil {
		return tasks
	}
	for rows.Next() {
		t := new(Task)
		rows.Scan(&t.ID, &t.Name, &t.Apiurl, &t.Event, &t.Stat, &t.CreateTime, &t.CreateUser, &t.RoutineCount, &t.ReRoutineCount, &t.ReSendTime, &t.RetryCount, &t.Timeout, &t.Desc)
		tasks.Set(t.ID, t)
	}
	s = "SELECT `id`, `schema`, `table`, `column`, `send`, `task_id` FROM `notify_field`"
	stmt, err = confdb.Prepare(s)
	sysLogger.LogErr(err)
	if err != nil {
		return tasks
	}
	rows, err = stmt.Query()
	sysLogger.LogErr(err)
	if err != nil {
		return tasks
	}
	for rows.Next() {
		f := new(NotifyField)
		rows.Scan(&f.ID, &f.Schema, &f.Table, &f.Column, &f.Send, &f.TaskID)
		task := tasks.Get(f.TaskID)
		if task != nil {
			if task.Fields == nil {
				task.Fields = make([]*NotifyField, 0, 10)
			}
			task.Fields = append(task.Fields, f)
		}
	}
	return tasks
}
