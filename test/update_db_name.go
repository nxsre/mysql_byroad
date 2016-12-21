package main

import (
	"fmt"
	"log"

	"flag"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

//mysql -h10.0.12.44 -P6006 -ubyroad_swd -p0CX9VnXh
const (
	Username = "byroad_swd"
	Password = "0CX9VnXh"
	Host     = "10.0.12.44"
	Port     = 6006
	DBName   = "byroad"
)

func main() {
	flag.Parse()
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8&parseTime=true",
		Username, Password, Host, Port,
		DBName)
	db := sqlx.MustOpen("mysql", dsn)
	from := flag.Arg(0)
	to := flag.Arg(1)
	s := fmt.Sprintf("update task set db_instance_name=? where db_instance_name=?")
	log.Println(s)
	_, err := db.Exec(s, to, from)
	if err != nil {
		log.Println(err.Error())
	}
	s = fmt.Sprintf("update config set description=? where description=?")
	log.Println(s)
	_, err = db.Exec(s, to, from)
	if err != nil {
		log.Println(err.Error())
	}
}
