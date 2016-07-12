package main

import (
	"fmt"
	"mysql_byroad/model"

	log "github.com/Sirupsen/logrus"
	"github.com/jmoiron/sqlx"
)

var done chan bool

func main() {
	server := NewRPCServer("tcp", "127.0.0.1:1234", "")
	server.start()
	client := NewRPCClient("tcp", "127.0.0.1:1235", "")
	/*
		task := model.Task{
			ID:     2,
			Name:   "test2",
			Apiurl: "http://localhost:8092/get",
			Stat:   "正常",
		}
		fields := make([]*model.NotifyField, 0, 10)
		field := model.NotifyField{
			Schema: "byroad",
			Table:  "hello",
			Column: "world",
		}
		fields = append(fields, &field)
		task.Fields = fields
		status, err := client.AddTask(&task)
		fmt.Println(status)
		if err != nil {
			panic(err)
		}*/
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8&parseTime=true",
		Conf.MysqlConf.Username, Conf.MysqlConf.Password, Conf.MysqlConf.Host, Conf.MysqlConf.Port, Conf.MysqlConf.DBName)
	confdb, err := sqlx.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	model.Init(confdb)
	dbmap, err := client.GetDBMap("byroad")
	if err != nil {
		log.Debug(err.Error())
	}
	log.Debugf("dbmap %+v", dbmap[0])
	fmt.Println(dbmap[0].Schema)
	for _, t := range dbmap[0].OrderedTables {
		fmt.Print(t.Table, " ")
		for _, c := range t.Columns {
			fmt.Print(c, " ")
		}
		fmt.Println()
	}
	<-done
}
