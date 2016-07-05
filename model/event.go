package model

import (
	"strings"
	"time"
)

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

/*
数据库-表-字段对应任务的一个订阅
*/
type NotifyField struct {
	ID         int64
	Schema     string
	Table      string
	Column     string
	Send       int
	TaskID     int64     `db:"task_id"`
	CreateTime time.Time `db:"create_time"`
}

func CreateNotifyFieldTable() {
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

type NotifyFields []*NotifyField

func (field *NotifyField) _insert() (id int64, err error) {
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

func (fields NotifyFields) _insert(taskID int64) error {
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

func (field *NotifyField) _delete() (int64, error) {
	s := "DELETE FROM `notify_field` WHERE `id`=?"
	res, err := confdb.Exec(s, field.ID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (fields NotifyFields) _delete(taskid int64) (int64, error) {
	s := "DELETE FROM `notify_field` WHERE `task_id`=?"
	res, err := confdb.Exec(s, taskid)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
