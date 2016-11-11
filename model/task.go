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
	DBInstanceName string           `db:"db_instance_name"` // 该任务所属的mysql实例
	PhoneNumbers   string           `db:"phone_numbers"`
	Emails         string           `db:"emails"`
	Alert          int              `db:"alert"`
	SubscribeStat  int              `db:"subscribe_stat"` // 任务是否开启订阅
	PushStat       int              `db:"push_stat"`      // 任务是否开启推送
}

func (task *Task) Insert() (id int64, err error) {
	s := "INSERT INTO `task_kafka`(`name`, `apiurl`, `event`, `stat`, `create_time`, `create_user`, `routine_count`, `re_routine_count`, `re_send_time`, `retry_count`, `timeout`, `desc`, `pack_protocal`, `db_instance_name`, `phone_numbers`, `emails`, `alert`, `subscribe_stat`, `push_stat`) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
	res, err := confdb.Exec(s, task.Name, task.Apiurl, task.Event, task.Stat,
		task.CreateTime, task.CreateUser, task.RoutineCount, task.ReRoutineCount,
		task.ReSendTime, task.RetryCount, task.Timeout, task.Desc, task.PackProtocal,
		task.DBInstanceName, task.PhoneNumbers, task.Emails, task.Alert, task.SubscribeStat,
		task.PushStat)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (task *Task) GetByID() (*Task, error) {
	fields := make([]*NotifyField, 0)
	s := "SELECT * FROM `task_kafka` WHERE `id`=?"
	err := confdb.Get(task, s, task.ID)
	if err != nil {
		return nil, err
	}
	s = "SELECT * FROM `notify_field_kafka` WHERE `task_id`=?"
	rows, err := confdb.Queryx(s, task.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		f := new(NotifyField)
		rows.StructScan(f)
		f.TaskID = task.ID
		fields = append(fields, f)
	}
	task.Fields = fields
	return task, nil
}

func (task *Task) Update() (int64, error) {
	task.Fields.Delete(task.ID)
	s := "UPDATE `task_kafka` SET `apiurl`=?, `event`=?, `name`=?, `stat`=?, `create_time`=?, `routine_count`=?, `re_routine_count`=?, `re_send_time`=?, `retry_count`=?, `timeout`=?, `desc`=?, `pack_protocal`=?,	`phone_numbers`=?, `emails`=?, `alert`=?, `subscribe_stat`=?, `push_stat`=? WHERE `id`=?"
	res, err := confdb.Exec(s, task.Apiurl, task.Event, task.Name, task.Stat, task.CreateTime,
		task.RoutineCount, task.ReRoutineCount, task.ReSendTime, task.RetryCount, task.Timeout,
		task.Desc, task.PackProtocal, task.PhoneNumbers, task.Emails, task.Alert, task.SubscribeStat,
		task.PushStat, task.ID)
	if err != nil {
		return 0, err
	}
	task.Fields.Insert(task.ID)
	return res.RowsAffected()
}

//delete task and its fields
func (task *Task) Delete() (int64, error) {
	s := "DELETE FROM `task_kafka` WHERE `id`=?"
	res, err := confdb.Exec(s, task.ID)
	if err != nil {
		return 0, err
	}
	s = "DELETE FROM `notify_field_kafka` WHERE `task_id`=?"
	res, err = confdb.Exec(s, task.ID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
func (task *Task) Get() (*Task, error) {
	return task.GetByID()
}
func (task *Task) SetStat() error {
	_, err := task.Update()
	if err != nil {
		return err
	}
	return nil
}

func (task *Task) SetSubscribeStat() error {
	s := "UPDATE `task_kafka` SET `subscribe_stat`=? where `id`=?"
	_, err := confdb.Exec(s, task.SubscribeStat, task.ID)
	return err
}

func (task *Task) SetPushStat() error {
	s := "UPDATE `task_kafka` SET `push_stat`=? where `id`=?"
	_, err := confdb.Exec(s, task.PushStat, task.ID)
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
	id, err = task.Insert()
	if err != nil {
		return
	}
	err = task.Fields.Insert(id)
	if err != nil {
		return
	}
	task.ID = id
	return
}

func GetAllTask() ([]*Task, error) {
	ts := []*Task{}
	err := confdb.Select(&ts, "SELECT * FROM `task_kafka`")
	if err != nil {
		return nil, err
	}
	fields := []*NotifyField{}
	err = confdb.Select(&fields, "SELECT * FROM `notify_field_kafka`")
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
	t, err := task.GetByID()
	if t != nil {
		return true, nil
	} else {
		return false, err
	}

}

func (task *Task) NameExists() (bool, error) {
	var cnt int
	err := confdb.Get(&cnt, "SELECT COUNT(*) FROM `task_kafka` WHERE name=?", task.Name)
	if err != nil {
		return false, err
	}
	if cnt == 1 {
		return true, nil
	} else {
		return false, nil
	}
}

func GetTasksByUser(createUser string) ([]*Task, error) {
	ts := []*Task{}
	err := confdb.Select(&ts, "SELECT * FROM `task_kafka` WHERE create_user=?", createUser)
	if err != nil {
		return nil, err
	}
	fields := []*NotifyField{}
	err = confdb.Select(&fields, "SELECT * FROM `notify_field_kafka`")
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

func GetTaskByInstanceName(name string) ([]*Task, error) {
	ts := []*Task{}
	err := confdb.Select(&ts, "SELECT * FROM `task_kafka` WHERE db_instance_name=?", name)
	if err != nil {
		return nil, err
	}
	fields := []*NotifyField{}
	err = confdb.Select(&fields, "SELECT * FROM `notify_field_kafka`")
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

func GetTasksByUserAndInstance(username, instance string) ([]*Task, error) {
	ts := []*Task{}
	err := confdb.Select(&ts, "SELECT * FROM `task_kafka` WHERE create_user=? AND db_instance_name=?", username, instance)
	if err != nil {
		return nil, err
	}
	fields := []*NotifyField{}
	err = confdb.Select(&fields, "SELECT * FROM `notify_field_kafka`")
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

func GetTaskByName(taskname string) (*Task, error) {
	task := Task{}
	err := confdb.Get(&task, "SELECT * FROM `task_kafka` WHERE name=?", taskname)
	return &task, err
}
