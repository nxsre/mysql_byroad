package main

import (
	"epg/log"
	"fmt"
	"mysql_byroad/model"

	"github.com/jmoiron/sqlx"
)

type TaskSlice []*model.Task

func (t TaskSlice) Len() int {
	return len(t)
}

func (t TaskSlice) Less(i, j int) bool {
	return int64(t[i].CreateTime.Sub(t[j].CreateTime)) > 0
}

func (t TaskSlice) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

type TaskManager struct {
	Host     string
	Port     uint16
	Username string
	Password string
	dsn      string
}

func NewTaskManager() *TaskManager {
	tm := &TaskManager{
		Host:     Conf.MysqlConf.Host,
		Port:     Conf.MysqlConf.Port,
		Username: Conf.MysqlConf.Username,
		Password: Conf.MysqlConf.Password,
	}
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8&parseTime=true",
		Conf.MysqlConf.Username, Conf.MysqlConf.Password, Conf.MysqlConf.Host, Conf.MysqlConf.Port, Conf.MysqlConf.DBName)
	tm.dsn = dsn
	confdb, err := sqlx.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	// 初始化话Model环境
	model.Init(confdb)
	return tm
}

func (tm *TaskManager) GetAllTask() ([]*model.Task, error) {
	db, err := sqlx.Open("mysql", tm.dsn)
	defer db.Close()
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}
	ts := []*model.Task{}
	err = db.Select(&ts, "SELECT * FROM `task`")
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}
	fields := []*model.NotifyField{}
	err = db.Select(&fields, "SELECT * FROM `notify_field`")
	if err != nil {
		log.Error(err.Error())
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
