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
	AuditState int       `db:"audit_state"`
	TaskID     int64     `db:"task_id"`
	AuditId    int64     `db:"audit_id"`
	CreateTime time.Time `db:"create_time"`
}

type NotifyFields []*NotifyField

func (fields NotifyFields) insert(taskID int64) error {
	if len(fields) == 0 {
		return nil
	}
	s := "INSERT INTO `notify_field`(`schema`, `table`, `column`, `send`, `audit_state`, `task_id`, `audit_id`, `create_time`) VALUES"
	fs := []interface{}{}
	for _, f := range fields {
		f.TaskID = taskID
		s += "(?, ?, ?, ?, ?, ?, ?, ?),"
		fs = append(fs, f.Schema, f.Table, f.Column, f.Send, f.AuditState, f.TaskID, f.AuditId, time.Now())
	}
	s = strings.TrimRight(s, ",")
	stmt, err := confdb.Prepare(s)
	if err != nil {
		return err
	}
	defer stmt.Close()
	res, err := stmt.Exec(fs...)
	if err != nil {
		return err
	}
	_, err = res.RowsAffected()
	return err
}

func (fields NotifyFields) Add(taskid int64) error {
	return fields.insert(taskid)
}

func (fields NotifyFields) delete(taskid int64) error {
	s := "DELETE FROM `notify_field` WHERE `task_id`=?"
	_, err := confdb.Exec(s, taskid)
	return err
}

func (field NotifyFields) deleteApproved(taskid int64) error {
	s := "DELETE FROM `notify_field` WHERE `task_id`=? AND `audit_state`=?"
	_, err := confdb.Exec(s, taskid, AUDIT_STATE_APPROVED)
	return err
}

func (field NotifyFields) deleteEnabled(taskid int64) error {
	s := "DELETE FROM `notify_field` WHERE `task_id`=? AND `audit_state`=?"
	_, err := confdb.Exec(s, taskid, AUDIT_STATE_ENABLED)
	return err
}

func (field NotifyFields) updateAuditState(taskid int64, state int) error {
	s := "UPDATE `notify_field` SET `audit_state`=? WHERE `task_id`=?"
	_, err := confdb.Exec(s, state, taskid)
	return err
}
