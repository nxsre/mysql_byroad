package main

import (
	"flag"
	"fmt"
	"mysql_byroad/model"
	"mysql_byroad/notice"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	log "github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Config struct {
	lookupdAddrs    string
	nsqdAddrs       string
	noticeUser      string
	noticePasswd    string
	smsAddr         string
	emailAddr       string
	maxChannleDepth int64
	lookupInterval  time.Duration
	phoneNumbers    string
	emails          string

	mysqlUsername string
	mysqlPassword string
	mysqlHost     string
	mysqlPort     int
	mysqlDBName   string
}

var Conf Config
var config string

func init() {
	flag.StringVar(&Conf.lookupdAddrs, "lookupdAddrs", "127.0.0.1:4161", "nsq lookupd addrs")
	flag.StringVar(&Conf.nsqdAddrs, "nsqdAddrs", "", "nsqd addrs")
	flag.StringVar(&Conf.noticeUser, "alert-user", "", "alert username")
	flag.StringVar(&Conf.noticePasswd, "alert-passwd", "", "alert password")
	flag.StringVar(&Conf.smsAddr, "sms-addr", "", "sms alert addr")
	flag.StringVar(&Conf.emailAddr, "email-addr", "", "email alert addr")
	flag.Int64Var(&Conf.maxChannleDepth, "max-channel-depth", 10000, "max channel depth to alert")
	flag.DurationVar(&Conf.lookupInterval, "lookup-interval", time.Minute, "lookup nsqd stats interval")
	flag.StringVar(&Conf.phoneNumbers, "alert-phone-numbers", "", "phone numbers to alert")
	flag.StringVar(&Conf.emails, "alert-emails", "", "emails to alert")

	flag.StringVar(&Conf.mysqlUsername, "mysql-username", "", "mysql username for byroad")
	flag.StringVar(&Conf.mysqlPassword, "mysql-password", "", "mysql password for byroad")
	flag.StringVar(&Conf.mysqlHost, "mysql-host", "localhost", "mysql host for byroad")
	flag.IntVar(&Conf.mysqlPort, "mysql-port", 3306, "mysql port for byroad")
	flag.StringVar(&Conf.mysqlDBName, "mysql-dbname", "byroad", "mysql db name")

	flag.StringVar(&config, "config", "", "config file for nsq monitor")
	flag.Parse()
}

func main() {
	if config != "" {
		_, err := toml.DecodeFile(config, &Conf)
		if err != nil {
			log.Fatal(err)
		}
	}
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8&parseTime=true", Conf.mysqlUsername, Conf.mysqlPassword, Conf.mysqlHost, Conf.mysqlPort, Conf.mysqlDBName)
	confdb, err := sqlx.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	model.Init(confdb)
	var lookups, nsqs []string
	if Conf.lookupdAddrs != "" {
		lookups = strings.Split(Conf.lookupdAddrs, ",")
	}
	if Conf.nsqdAddrs != "" {
		nsqs = strings.Split(Conf.nsqdAddrs, ",")
	}
	config := &MonitorConfig{
		MaxChannelDepth: Conf.maxChannleDepth,
		LookupInterval:  Conf.lookupInterval,
		PhoneNumbers:    Conf.phoneNumbers,
		Emails:          Conf.emails,
	}
	noticeConfig := &notice.Config{
		User:      Conf.noticeUser,
		Password:  Conf.noticePasswd,
		SmsAddr:   Conf.smsAddr,
		EmailAddr: Conf.emailAddr,
	}
	monitor := NewNSQMonitor(nsqs, lookups, config, noticeConfig)
	monitor.RunNSQDMonitor()
	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-exitChan
}
