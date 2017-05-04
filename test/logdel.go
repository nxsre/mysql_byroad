package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var (
	Username   string
	Password   string
	Host       string
	Port       int
	DBName     string
	StepCount  int
	StartCount int
	EndCount   int
)

var db *sqlx.DB
var affected int64

func main() {
	flag.StringVar(&Username, "u", "root", "username")
	flag.StringVar(&Password, "p", "", "password")
	flag.StringVar(&Host, "h", "127.0.0.1", "host")
	flag.IntVar(&Port, "P", 6006, "port")
	flag.StringVar(&DBName, "D", "byroad", "database")

	flag.IntVar(&StartCount, "start", 0, "delete start id")
	flag.IntVar(&StepCount, "step", 5000, "delete step")
	flag.IntVar(&EndCount, "end", 20000000, "delete end id")
	flag.Parse()
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8&parseTime=true",
		Username, Password, Host, Port,
		DBName)
	log.Printf("[INFO] connect to %s", dsn)
	db = sqlx.MustOpen("mysql", dsn)
	for StartCount < EndCount {
		delLog(StartCount, StartCount+StepCount)
		StartCount += StepCount
		rn := rand.Intn(4)
		log.Printf("sleep %d", (2 + rn))
		time.Sleep(time.Second * time.Duration((2 + rn)))
	}
	log.Printf("[INFO] total affected rows: %d", affected)
	log.Printf("done !")
	db.Close()
}

func delLog(start, end int) {
	sq := "delete from tasklog where id >= ? and id < ?"
	log.Printf(fmt.Sprintf("[INFO] delete from tasklog where id >= %d and id < %d", start, end))
	rs, err := db.Exec(sq, start, end)
	if err != nil {
		log.Fatal(err.Error())
	}
	a, err := rs.RowsAffected()
	if err != nil {
		log.Fatal(err.Error())
	}
	affected += a
	log.Printf("[INFO] affected rows: %d", a)
}
