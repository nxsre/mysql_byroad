package main

import (
	"fmt"
	"log"
	"mysql_byroad/model"

	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql"
)

//var host = "10.0.12.44"
//var port = 6006
//var username = "byroad_swd"
//var password = "0CX9VnXh"
var host = "localhost"
var port = 3306
var username = "root"
var password = "toor"
var dbname = "byroad"
var surfix = "-(yz)"
var fromName = "localhost"
var toName = "jumei_product"

func main() {
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8&parseTime=true", username, password, host, port, dbname)
	conn, err := sqlx.Open("mysql", dsn)
	if err != nil {
		log.Panic(err)
	}
	model.Init(conn)
	tasks, err := model.GetTaskByInstanceName(fromName)
	if err != nil {
		log.Panic(err)
	}
	for _, task := range tasks {
		task.Name = task.Name + surfix
		task.DBInstanceName = toName
		_, err := task.Insert()
		if err != nil {
			panic(err)
		}
	}
}
