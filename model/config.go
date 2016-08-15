package model

import (
	"database/sql"
)

func CreateConfigTable() {
	s := "CREATE TABLE IF NOT EXISTS `config` ( " +
		"`id` INTEGER PRIMARY KEY AUTO_INCREMENT," +
		"`key` varchar(120) NOT NULL," +
		"`value` varchar(120) NOT NULL," +
		"`description` varchar(120)" +
		");"
	confdb.MustExec(s)
}

/*
配置信息
*/
type Config struct {
	ID          int64
	Key         string
	Value       string
	Description string
}

func (_ *Config) Set(key, value, desc string) (int64, error) {
	var id int64
	err := confdb.Get(&id, "SELECT id FROM config WHERE `key`=?", key)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	var res sql.Result
	if id == 0 {
		res, err = confdb.Exec("INSERT INTO config(`key`, value, description) VALUES(?, ?, ?)", key, value, desc)
		if err != nil {
			return 0, err
		}
		return res.LastInsertId()
	} else {
		_, err = confdb.Exec("UPDATE config SET value=?, description=? WHERE `key`=?", value, desc, key)
		return id, err
	}
}

func (_ *Config) Get(key string) (string, error) {
	var value string
	err := confdb.Get(&value, "SELECT value FROM config WHERE `key`=?", key)
	return value, err
}
