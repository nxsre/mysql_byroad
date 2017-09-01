package model

import (
	"database/sql"
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
	Alert          int
	SubscribeState int       `db:"subscribe_state"` // 任务是否开启订阅
	PushState      int       `db:"push_state"`      // 任务是否开启推送
	AuditState     int       `db:"audit_state"`     // 任务审计状态
	UpdateTime     time.Time `db:"update_time"`
	Category       string    // 任务分组
}

func (task *Task) Add() error {
	s := "INSERT INTO `task`(`name`, `apiurl`, `event`, `stat`, `create_time`, `create_user`, `routine_count`, `re_routine_count`, `re_send_time`, `retry_count`, `timeout`, `desc`, `pack_protocal`, `db_instance_name`, `phone_numbers`, `emails`, `alert`, `audit_state` `push_state`, `update_time`, `category`) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
	res, err := confdb.Exec(s, task.Name, task.Apiurl, task.Event, task.Stat,
		time.Now(), task.CreateUser, task.RoutineCount, task.ReRoutineCount,
		task.ReSendTime, task.RetryCount, task.Timeout, task.Desc, task.PackProtocal,
		task.DBInstanceName, task.PhoneNumbers, task.Emails, task.Alert, task.AuditState,
		task.PushState, time.Now(), task.Category)
	if err != nil {
		return err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	task.ID = id
	return nil
}

func (task *Task) AddWithFields() error {
	err := task.Add()
	if err != nil {
		return err
	}
	return task.Fields.Add(task.ID)
}

func (task *Task) Delete() error {
	s := "DELETE FROM `task` WHERE `id`=?"
	_, err := confdb.Exec(s, task.ID)
	return err
}

func (task *Task) DeleteWithFields() error {
	s := "DELETE FROM `task` WHERE `id`=?"
	_, err := confdb.Exec(s, task.ID)
	if err != nil {
		return err
	}
	return task.Fields.delete(task.ID)
}

func (task *Task) Update() error {
	s := "UPDATE `task` SET `name`=?, `apiurl`=?, `event`=?, `stat`=?, `routine_count`=?, `re_routine_count`=?, `re_send_time`=?, `retry_count`=?, `timeout`=?, `desc`=?, `pack_protocal`=?, `phone_numbers`=?, `emails`=?, `alert`=?, `push_state`=?, `update_time`=?, `category`=? WHERE `id`=?"
	_, err := confdb.Exec(s, task.Name, task.Apiurl, task.Event, task.Stat, task.RoutineCount,
		task.ReRoutineCount, task.ReSendTime, task.RetryCount, task.Timeout, task.Desc, task.PackProtocal,
		task.PhoneNumbers, task.Emails, task.Alert, task.PushState, time.Now(), task.Category, task.ID)
	return err
}

func (task *Task) UpdateWithField() error {
	task.Fields.delete(task.ID)
	s := "UPDATE `task` SET `apiurl`=?, `event`=?, `name`=?, `stat`=?, `routine_count`=?, `re_routine_count`=?, `re_send_time`=?, `retry_count`=?, `timeout`=?, `desc`=?, `pack_protocal`=?, `phone_numbers`=?, `emails`=?, `alert`=?, `push_state`=?, `update_time`=?, `category`=? WHERE `id`=?"
	_, err := confdb.Exec(s, task.Apiurl, task.Event, task.Name, task.Stat, task.RoutineCount,
		task.ReRoutineCount, task.ReSendTime, task.RetryCount, task.Timeout, task.Desc, task.PackProtocal,
		task.PhoneNumbers, task.Emails, task.Alert, task.PushState, time.Now(), task.Category, task.ID)
	if err != nil {
		return err
	}
	return task.Fields.insert(task.ID)
}

func (task *Task) GetById() error {
	s := "SELECT * FROM `task` WHERE `id`=?"
	return confdb.Get(task, s, task.ID)
}

func (task *Task) GetByName() error {
	return confdb.Get(task, "SELECT * FROM `task` WHERE name=?", task.Name)
}

func (task *Task) UpdateStat() error {
	sql := "UPDATE `task` SET `stat`=? WHERE `id`=?"
	_, err := confdb.Exec(sql, task.Stat, task.ID)
	return err
}

func (task *Task) UpdatePushState() error {
	sql := "UPDATE `task` SET `push_state`=? WHERE `id`=?"
	_, err := confdb.Exec(sql, task.PushState, task.ID)
	return err
}

func (task *Task) UpdateAuditState() error {
	sql := "UPDATE `task` SET `audit_state`=? WHERE `id`=?"
	_, err := confdb.Exec(sql, task.AuditState, task.ID)
	if err != nil {
		return err
	}
	return task.Fields.updateAuditState(task.ID, task.AuditState)
}

func (task *Task) DeleteApprovedTaskFields() error {
	return task.Fields.deleteApproved(task.ID)
}

func (task *Task) DeleteEnabledTaskFields() error {
	return task.Fields.deleteEnabled(task.ID)
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

func (task *Task) IdExists() bool {
	t := &Task{}
	s := "SELECT `id` FROM `task` WHERE `id`=?"
	err := confdb.Get(t, s, task.ID)
	if err == sql.ErrNoRows {
		return false
	}
	return true
}

func (task *Task) NameExists() bool {
	t := &Task{}
	s := "SELECT `id` FROM `task` WHERE `name`=?"
	err := confdb.Get(t, s, task.Name)
	if err == sql.ErrNoRows {
		return false
	}
	return true
}

func (task *Task) UpdateCreateUser() error {
	s := "UPDATE `task` SET `create_user`=? WHERE `id`=?"
	_, err := confdb.Exec(s, task.CreateUser, task.ID)
	return err
}

func GetTasksByUser(createUser string) ([]*Task, error) {
	ts := []*Task{}
	err := confdb.Select(&ts, "SELECT * FROM `task` WHERE create_user=?", createUser)
	return ts, err
}

func GetTaskByInstanceName(name string) ([]*Task, error) {
	ts := []*Task{}
	err := confdb.Select(&ts, "SELECT * FROM `task` WHERE db_instance_name=?", name)
	return ts, err
}

func GetTasksByUserAndInstance(username, instance string) ([]*Task, error) {
	ts := []*Task{}
	err := confdb.Select(&ts, "SELECT * FROM `task` WHERE create_user=? AND db_instance_name=?", username, instance)
	return ts, err
}

func GetTaskByName(taskname string) (*Task, error) {
	task := Task{}
	err := confdb.Get(&task, "SELECT * FROM `task` WHERE name=?", taskname)
	return &task, err
}

func GetEnabledTasksByInstance(instance string) ([]*Task, error) {
	ts := []*Task{}
	sql := "SELECT * FROM `task` WHERE `audit_state`=? AND `db_instance_name`=? ORDER BY `update_time`"
	err := confdb.Select(&ts, sql, AUDIT_STATE_ENABLED, instance)
	return ts, err
}

func GetEnabledTasksByInstanceAndUser(instance string, username string) ([]*Task, error) {
	ts := []*Task{}
	s := "SELECT * FROM `task` WHERE `audit_state`=? AND `db_instance_name`=? AND `create_user`=? ORDER BY `update_time`"
	err := confdb.Select(&ts, s, AUDIT_STATE_ENABLED, instance, username)
	return ts, err
}

func GetEnabledTasksByCategory(category string) ([]*Task, error) {
	ts := []*Task{}
	s := "SELECT * FROM `task` WHERE `audit_state`=? AND `category`=? ORDER BY `update_time`"
	err := confdb.Select(&ts, s, AUDIT_STATE_ENABLED, category)
	return ts, err
}

func GetEnabledTasksWithFieldsByInstance(instance string) ([]*Task, error) {
	ts := []*Task{}
	err := confdb.Select(&ts, "SELECT * FROM `task` WHERE `db_instance_name`=?", instance)
	if err != nil {
		return nil, err
	}
	fields := []*NotifyField{}
	err = confdb.Select(&fields, "SELECT * FROM `notify_field` WHERE `audit_state`=?", AUDIT_STATE_ENABLED)
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

func (t *Task) GetWithFieldsState(state int) error {
	sql := "SELECT * FROM `task` WHERE `id`=?"
	err := confdb.Get(t, sql, t.ID)
	if err != nil {
		return err
	}
	fields := []*NotifyField{}
	err = confdb.Select(&fields, "SELECT * FROM `notify_field` WHERE `task_id`=? AND `audit_state`=?", t.ID, state)
	t.Fields = fields
	return err
}

func (t *Task) GetWithFieldsEnabled() error {
	return t.GetWithFieldsState(AUDIT_STATE_ENABLED)
}

func (t *Task) GetWithFieldsUnenabled() error {
	sql := "SELECT * FROM `task` WHERE `id`=?"
	err := confdb.Get(t, sql, t.ID)
	if err != nil {
		return err
	}
	fields := []*NotifyField{}
	err = confdb.Select(&fields, "SELECT * FROM `notify_field` WHERE `task_id`=? AND `audit_state`!=?", t.ID, AUDIT_STATE_ENABLED)
	t.Fields = fields
	return err
}
