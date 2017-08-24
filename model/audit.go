package model

import "time"

type Audit struct {
	Id         int64
	ApplyUser  string    `db:"apply_user"`
	AuditUser  string    `db:"audit_user"`
	ApplyType  int       `db:"apply_type"`
	State      int       `db:"state"`
	TaskId     int64     `db:"task_id"`
	CreateTime time.Time `db:"create_time"`
	UpdateTime time.Time `db:"update_time"`
}

const (
	AUDIT_STATE_UNHANDLE = iota
	AUDIT_STATE_PENDING
	AUDIT_STATE_APPROVED
	AUDIT_STATE_DENYED

	AUDIT_TYPE_CREATE = iota
	AUDIT_TYPE_UPDATE
	AUDIT_TYPE_DELETE
)

func (a *Audit) Add() (int64, error) {
	sql := "INSERT INTO `audit` (`apply_user`, `audit_user`, `apply_type`, `state`, `task_id`, `create_time`) VALUES (?,?,?,?,?,?)"
	ret, err := confdb.Exec(sql, a.ApplyUser, a.AuditUser, a.ApplyType, a.State, a.TaskId, time.Now())
	if err != nil {
		return 0, err
	}
	return ret.LastInsertId()
}

func (a *Audit) UpdateState() (int64, error) {
	sql := "UPDATE `audit` SET `state`=?, `update_time`=? where `id`=?"
	ret, err := confdb.Exec(sql, a.State, time.Now(), a.Id)
	if err != nil {
		return 0, err
	}
	return ret.RowsAffected()
}

func (a *Audit) Delete() (int64, error) {
	sql := "DELETE FROM `audit` WHERE `id`=?"
	ret, err := confdb.Exec(sql, a.Id)
	if err != nil {
		return 0, err
	}
	return ret.RowsAffected()
}

func GetAuditByApplyUser(user string) ([]*Audit, error) {
	as := []*Audit{}
	sql := "SELECT * FROM `audit` WHERE `apply_user`=? ORDER BY `create_time` DESC"
	err := confdb.Select(&as, sql, user)
	return as, err
}

func GetAuditByAuditUser(user string) ([]*Audit, error) {
	as := []*Audit{}
	sql := "SELECT * FROM `audit` WHERE `audit_user`=? ORDER BY `create_time` DESC"
	err := confdb.Select(&as, sql, user)
	return as, err
}

func GetAllAudits() ([]*Audit, error) {
	as := []*Audit{}
	sql := "SELECT * FROM `audit` ORDER BY `create_time` DESC"
	err := confdb.Select(&as, sql)
	return as, err
}
