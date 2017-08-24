package model

import "time"

type Audit struct {
	Id         int
	ApplyUser  string `db:"apply_user"`
	AuditUser  string `db:"audit_user"`
	ApplyQuery string `db:"apply_query"`
	ApplyType  int    `db:"apply_type"`
	State      int    `db:"state"`
	CreateTime string `db:"create_time"`
}

func (a *Audit) Add() (int64, error) {
	sql := "INSERT INTO `audit` (`apply_user`, `audit_user`, `apply_query`, `apply_type`, `state`, `create_time`) VALUES (?,?,?,?,?,?)"
	ret, err := confdb.Exec(sql, a.ApplyUser, a.AuditUser, a.ApplyQuery, a.ApplyType, a.State, time.Now())
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
	sql := "SELECT * FROM `audit` WHERE `apply_user`=?"
	err := confdb.Select(&as, sql, user)
	return as, err
}

func GetAuditByAuditUser(user string) ([]*Audit, error) {
	as := []*Audit{}
	sql := "SELECT * FROM `audit` WHERE `audit_user`=?"
	err := confdb.Select(&as, sql, user)
	return as, err
}
