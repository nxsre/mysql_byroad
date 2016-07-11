package main

import "mysql_byroad/model"

var done chan bool

func main() {
	server := NewRPCServer("tcp", "127.0.0.1:1234", "")
	server.start()
	client := NewRPCClient("tcp", "127.0.0.1:1235", "")
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
	client.AddTask(&task)
	<-done
}
