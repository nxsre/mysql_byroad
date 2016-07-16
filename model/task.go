package model

import (
	"time"
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
	CreateTime     time.Time    `db:"create_time"`
	CreateUser     string       `db:"create_user"`
	RoutineCount   int          `db:"routine_count"`    //推送协程数
	ReRoutineCount int          `db:"re_routine_count"` //重推协程数
	ReSendTime     int          `db:"re_send_time"`     //重推时间间隔
	RetryCount     int          `db:"retry_count"`      //重推次数
	Timeout        int          //消息处理超时
	QueueLength    int64
	ReQueueLength  int64
	Desc           string
	Statistic      *Statistic
	PackProtocal   DataPackProtocal `db:"pack_protocal"`
}

func CreateTaskTable() {
	s := "CREATE TABLE IF NOT EXISTS `task` (" +
		"`id` INTEGER PRIMARY KEY AUTO_INCREMENT," +
		"`name` VARCHAR(120) NOT NULL," +
		"`apiurl` VARCHAR(120) NOT NULL," +
		"`event` VARCHAR(120) NOT NULL," +
		"`stat` VARCHAR(120) NOT NULL," +
		"`create_time` DATETIME NOT NULL," +
		"`create_user` VARCHAR(120) NOT NULL," +
		"`routine_count` INTEGER NOT NULL," +
		"`re_routine_count` INTEGER NOT NULL," +
		"`re_send_time` INTEGER NOT NULL," +
		"`retry_count` INTEGER NOT NULL," +
		"`timeout` INTEGER NOT NULL," +
		"`desc` VARCHAR(255)," +
		"`pack_protocal` INTEGER" +
		")"
	confdb.MustExec(s)
}

func (task *Task) _insert() (id int64, err error) {
	s := "INSERT INTO `task`(`name`, `apiurl`, `event`, `stat`, `create_time`, `create_user`, `routine_count`, `re_routine_count`, `re_send_time`, `retry_count`, `timeout`, `desc`, `pack_protocal`) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
	res, err := confdb.Exec(s, task.Name, task.Apiurl, task.Event, task.Stat, task.CreateTime, task.CreateUser, task.RoutineCount, task.ReRoutineCount, task.ReSendTime, task.RetryCount, task.Timeout, task.Desc, task.PackProtocal)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (task *Task) _getByID() (*Task, error) {
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

func (task *Task) _update() (int64, error) {
	task.Fields._delete(task.ID)
	s := "UPDATE `task` SET `apiurl`=?, `event`=?, `name`=?, `stat`=?, `create_time`=?, `routine_count`=?, `re_routine_count`=?, `re_send_time`=?, `retry_count`=?, `timeout`=?, `desc`=?, `pack_protocal`=? WHERE `id`=?"
	res, err := confdb.Exec(s, task.Apiurl, task.Event, task.Name, task.Stat, task.CreateTime, task.RoutineCount, task.ReRoutineCount, task.ReSendTime, task.RetryCount, task.Timeout, task.Desc, task.PackProtocal, task.ID)
	if err != nil {
		return 0, err
	}
	task.Fields._insert(task.ID)
	return res.RowsAffected()
}

//delete task and its fields
func (task *Task) _delete() (int64, error) {
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
func (task *Task) Get() (*Task, error) {
	return task._getByID()
}
func (task *Task) SetStat() error {
	_, err := task._update()
	if err != nil {
		return err
	}
	return nil
}

func (task *Task) Update() error {
	_, err := task._update()
	if err != nil {
		return err
	}
	return nil
}

func (task *Task) Delete() error {
	_, err := task._delete()
	return err
}

func (this *Task) FieldExists(field *NotifyField) bool {
	for _, f := range this.Fields {
		if f.Schema == field.Schema && f.Table == field.Table && f.Column == field.Column {
			return true
		}
	}
	return false
}

func (task *Task) GetTaskColumnsMap() map[string]map[string]NotifyFields {
	colsMap := make(map[string]map[string]NotifyFields)
	for _, field := range task.Fields {
		if colsMap[field.Schema] == nil {
			colsMap[field.Schema] = make(map[string]NotifyFields)
			colsMap[field.Schema][field.Table] = *new(NotifyFields)
		}
		colsMap[field.Schema][field.Table] = append(colsMap[field.Schema][field.Table], field)
	}
	return colsMap
}

func (task *Task) Add() (id int64, err error) {
	id, err = task._insert()
	if err != nil {
		return
	}
	err = task.Fields._insert(id)
	if err != nil {
		return
	}
	task.ID = id
	return
}

func GetAllTask() ([]*Task, error) {
	ts := []*Task{}
	err := confdb.Select(&ts, "SELECT * FROM `task`")
	if err != nil {
		return nil, err
	}
	fields := []*NotifyField{}
	err = confdb.Select(&fields, "SELECT * FROM `notify_field`")
	if err != nil {
		return nil, err
	}
	for _, task := range ts {
		for _, field := range fields {
			if task.ID == field.TaskID {
				task.Fields = append(task.Fields, field)
			}
		}
	}
	return ts, nil
}

func (task *Task) Exists() (bool, error) {
	t, err := task._getByID()
	if t != nil {
		return true, nil
	} else {
		return false, err
	}

}

func (task *Task) NameExists() (bool, error) {
	var cnt int
	err := confdb.Get(&cnt, "SELECT COUNT(*) FROM task WHERE name=?", task.Name)
	if err != nil {
		return false, err
	}
	if cnt == 1 {
		return true, nil
	} else {
		return false, nil
	}
}

func GetTasks(createUser string) ([]*Task, error) {
	ts := []*Task{}
	err := confdb.Select(&ts, "SELECT * FROM `task` WHERE create_user=?", createUser)
	if err != nil {
		return nil, err
	}
	fields := []*NotifyField{}
	err = confdb.Select(&fields, "SELECT * FROM `notify_field`")
	if err != nil {
		return nil, err
	}
	for _, task := range ts {
		for _, field := range fields {
			if task.ID == field.TaskID {
				task.Fields = append(task.Fields, field)
			}
		}
	}
	return ts, nil
}
