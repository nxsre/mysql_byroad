package model

import (
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	log "github.com/Sirupsen/logrus"
)

// AddTask 用户新增任务操作；需要添加任务信息，订阅字段信息，审核信息
func AddTaskWithAudit(task *Task, audit *Audit) (err error) {
	tx, err := confdb.Beginx()
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			err := tx.Rollback()
			if err != nil {
				log.Errorf("add task with audit: %s", err.Error())
			}
		}
	}()

	err = addTask(tx, task)
	if err != nil {
		return
	}
	audit.TaskId = task.ID
	err = addAudit(tx, audit)
	if err != nil {
		return
	}

	err = addTaskFields(tx, task, audit)
	if err != nil {
		return
	}

	err = tx.Commit()
	return
}

// UpdateTask 用户更新任务，包括订阅字段信息；需要更新任务信息，添加订阅的字段信息，审核信息
func UpdateTaskFieldsWithAudit(task *Task, audit *Audit) (err error) {
	tx, err := confdb.Beginx()
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			err := tx.Rollback()
			if err != nil {
				log.Errorf("update task fields with audit: %s", err.Error())
			}
		}
	}()

	audit.TaskId = task.ID
	err = addAudit(tx, audit)
	if err != nil {
		return
	}
	err = addTaskFields(tx, task, audit)
	if err != nil {
		return
	}
	err = tx.Commit()
	return
}

// UpdateAuditState 更新审计的状态，包括订阅字段的状态
func UpdateAuditState(audit *Audit) (err error) {
	tx, err := confdb.Beginx()
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			err := tx.Rollback()
			if err != nil {
				log.Errorf("update audit state: %s", err.Error())
			}
		}
	}()
	err = updateAuditStateById(tx, audit)
	if err != nil {
		return
	}
	err = updateTaskFieldsAuditStateByAuditId(tx, audit)
	if err != nil {
		return
	}
	err = tx.Commit()
	return
}

// EnableAudit 启用审计，包括启用任务，启用订阅的字段
func EnableAudit(audit *Audit) (err error) {
	tx, err := confdb.Beginx()
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			err := tx.Rollback()
			if err != nil {
				log.Errorf("enable audit: %s", err.Error())
			}
		}
	}()
	err = unenableTaskFields(tx, audit)
	if err != nil {
		return
	}
	err = updateTaskAuditState(tx, audit)
	if err != nil {
		return
	}
	err = updateTaskFieldsAuditStateByAuditId(tx, audit)
	if err != nil {
		return
	}
	err = updateAuditStateById(tx, audit)
	if err != nil {
		return
	}

	err = tx.Commit()
	return
}

func AddTaskFields(task *Task) (err error) {
	tx, err := confdb.Beginx()
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			err := tx.Rollback()
			if err != nil {
				log.Errorf("enable audit: %s", err.Error())
			}
		}
	}()

	err = addTask(tx, task)
	if err != nil {
		return
	}

	audit := &Audit{}

	err = addTaskFields(tx, task, audit)
	if err != nil {
		return
	}

	err = tx.Commit()
	return
}

func GetTaskFieldsByAudit(audit *Audit) (*Task, error) {
	fields := []*NotifyField{}
	task := &Task{}
	err := confdb.Get(task, "SELECT * FROM `task` WHERE `id`=?", audit.TaskId)
	if err != nil {
		return task, err
	}
	err = confdb.Select(&fields, "SELECT * FROM `notify_field` WHERE `audit_id`=?", audit.Id)
	task.Fields = fields
	return task, err
}

func addTask(tx *sqlx.Tx, task *Task) (err error) {
	sql := "INSERT INTO `task` (`name`, `apiurl`, `event`, `stat`, `create_time`, `create_user`, `routine_count`, `re_routine_count`, `re_send_time`, `retry_count`, `timeout`, `desc`, `pack_protocal`, `db_instance_name`, `phone_numbers`, `emails`, `alert`, `audit_state`, `push_state`, `update_time`) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
	res, err := tx.Exec(sql, task.Name, task.Apiurl, task.Event, task.Stat,
		time.Now(), task.CreateUser, task.RoutineCount, task.ReRoutineCount,
		task.ReSendTime, task.RetryCount, task.Timeout, task.Desc, task.PackProtocal,
		task.DBInstanceName, task.PhoneNumbers, task.Emails, task.Alert, task.AuditState,
		task.PushState, time.Now())
	if err != nil {
		return
	}
	id, err := res.LastInsertId()
	if err != nil {
		return
	}
	task.ID = id
	return
}

func addAudit(tx *sqlx.Tx, audit *Audit) (err error) {
	sql := "INSERT INTO `audit` (`apply_user`, `audit_user`, `apply_type`, `state`, `task_id`, `create_time`, `update_time`) VALUES (?,?,?,?,?,?,?)"
	res, err := tx.Exec(sql, audit.ApplyUser, audit.AuditUser, audit.ApplyType, audit.State, audit.TaskId, time.Now(), time.Now())
	if err != nil {
		return
	}
	id, err := res.LastInsertId()
	if err != nil {
		return
	}
	audit.Id = id
	return
}

func addTaskFields(tx *sqlx.Tx, task *Task, audit *Audit) (err error) {
	if len(task.Fields) == 0 {
		return
	}
	s := "INSERT INTO `notify_field` (`schema`, `table`, `column`, `send`, `audit_state`, `task_id`, `audit_id`, `create_time`, `update_time`) VALUES"
	fs := []interface{}{}
	for _, f := range task.Fields {
		f.TaskID = task.ID
		f.AuditId = audit.Id
		s += "(?, ?, ?, ?, ?, ?, ?, ?, ?),"
		fs = append(fs, f.Schema, f.Table, f.Column, f.Send, f.AuditState, f.TaskID, f.AuditId, time.Now(), time.Now())
	}
	s = strings.TrimRight(s, ",")
	_, err = tx.Exec(s, fs...)
	return err
}

func updateAuditStateById(tx *sqlx.Tx, audit *Audit) (err error) {
	sql := "UPDATE `audit` SET `state`=?, `update_time`=? WHERE `id`=?"
	_, err = tx.Exec(sql, audit.State, time.Now(), audit.Id)
	return
}

func updateTaskFieldsAuditStateByAuditId(tx *sqlx.Tx, audit *Audit) (err error) {
	s := "UPDATE `notify_field` SET `audit_state`=?, `update_time`=? WHERE `audit_id`=?"
	_, err = tx.Exec(s, audit.State, time.Now(), audit.Id)
	return err
}

func updateTaskAuditState(tx *sqlx.Tx, audit *Audit) (err error) {
	s := "UPDATE `task` SET `audit_state`=?, `update_time`=? WHERE `id`=?"
	_, err = tx.Exec(s, audit.State, time.Now(), audit.TaskId)
	return
}

func unenableTaskFields(tx *sqlx.Tx, audit *Audit) (err error) {
	s := "UPDATE `notify_field` SET `audit_state`=?, `update_time`=? WHERE `audit_state`=?"
	_, err = tx.Exec(s, AUDIT_STATE_UNENABLED, time.Now(), AUDIT_STATE_ENABLED)
	return
}
