package model

import (
	"database/sql"
	"time"
)

type User struct {
	Id          int64
	Username    string
	Fullname    string
	Role        int
	Permissions string
	Mail        string
	CreateTime  time.Time `db:"create_time"`
	UpdateTime  time.Time `db:"update_time"`
}

const (
	USER_NORMAL = iota
	USER_AUDIT
	USER_ADMIN
	USER_SUPER
)

func (u *User) Add() error {
	sql := "INSERT INTO `user` (`username`, `fullname`, `role`, `permissions`, `mail`, `create_time`, `update_time`) VALUES (?,?,?,?,?,?,?)"
	ret, err := confdb.Exec(sql, u.Username, u.Fullname, u.Role, u.Permissions, u.Mail, time.Now(), time.Now())
	if err != nil {
		return err
	}
	id, err := ret.LastInsertId()
	u.Id = id
	return err
}

func (u *User) UpdateRole() (int64, error) {
	sql := "UPDATE `user` SET `role`=?, `update_time`=? where `id`=?"
	ret, err := confdb.Exec(sql, u.Role, time.Now(), u.Id)
	if err != nil {
		return 0, err
	}
	return ret.RowsAffected()
}

func (u *User) Delete() (int64, error) {
	sql := "DELETE FROM `user` WHERE `id`=?"
	ret, err := confdb.Exec(sql, u.Id)
	if err != nil {
		return 0, err
	}
	return ret.RowsAffected()
}

func GetAllUsers() ([]*User, error) {
	us := []*User{}
	err := confdb.Select(&us, "SELECT * FROM `user` order by `username`")
	return us, err
}

func (u *User) NameExists() bool {
	user := &User{}
	s := "SELECT `id` FROM `user` WHERE `username`=?"
	err := confdb.Get(user, s, u.Username)
	if err == sql.ErrNoRows {
		return false
	}
	return true
}

func (u *User) IdExists() bool {
	user := &User{}
	s := "SELECT `id` FROM `user` WHERE `id`=?"
	err := confdb.Get(user, s, u.Id)
	if err == sql.ErrNoRows {
		return false
	}
	return true
}

func (u *User) GetByName() error {
	sql := "SELECT * FROM `user` WHERE `username`=?"
	err := confdb.Get(u, sql, u.Username)
	return err
}

func (u *User) GetById() error {
	sql := "SELECT * FROM `user` WHERE `id`=?"
	err := confdb.Get(u, sql, u.Id)
	return err
}

func (u *User) GetOrAdd() error {
	exists := u.NameExists()
	if !exists {
		u.Role = USER_NORMAL
		return u.Add()
	}
	return u.GetByName()
}

func GetUsersByRole(role int) ([]*User, error) {
	sql := "SELECT * FROM `user` WHERE `role` >= ?"
	us := []*User{}
	err := confdb.Select(&us, sql, role)
	return us, err
}
