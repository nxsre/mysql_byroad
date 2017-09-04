package main

import (
	"flag"
	"fmt"
	"log"
	"mysql_byroad/model"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// update task set audit_state=4 push_state=stat update_time category=db_instance_name
// update notify_field set audit_state=4 audit_id=0 update_time

type MysqlConfig struct {
	Username string
	Password string
	Host     string
	Port     int
	DBName   string
}

var conf MysqlConfig

func init() {
	flag.StringVar(&conf.Username, "u", "root", "username")
	flag.StringVar(&conf.Password, "p", "123456", "password")
	flag.StringVar(&conf.Host, "h", "127.0.0.1", "host")
	flag.IntVar(&conf.Port, "P", 3306, "port")
	flag.StringVar(&conf.DBName, "D", "byroad", "db name")
	flag.Parse()
}

func main() {
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8&parseTime=true",
		conf.Username, conf.Password, conf.Host, conf.Port,
		conf.DBName)
	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		log.Panic(err)
	}
	model.Init(db)
	tasks, err := model.GetAllTask()
	if err != nil {
		log.Panic(err.Error())
	}
	for _, task := range tasks {
		s := "update `task` set `audit_state`=?, `push_state`=?, `update_time`=?, `category`=? where `id`=?"
		var pushState int
		if task.Stat == "正常" {
			pushState = model.TASK_STAT_PUSH
		} else {
			pushState = model.TASK_STAT_UNPUSH
		}
		log.Println(s, model.AUDIT_STATE_ENABLED, pushState, task.CreateTime, task.DBInstanceName, task.ID)
		db.MustExec(s, model.AUDIT_STATE_ENABLED, pushState, task.CreateTime, task.DBInstanceName, task.ID)
		for _, f := range task.Fields {
			s := "update `notify_field` set `audit_state`=?, `audit_id`=?, `update_time`=? where `id`=?"
			log.Println(s, model.AUDIT_STATE_ENABLED, 0, f.CreateTime, f.ID)
			db.MustExec(s, model.AUDIT_STATE_ENABLED, 0, f.CreateTime, f.ID)
		}
		log.Println()
	}
}
