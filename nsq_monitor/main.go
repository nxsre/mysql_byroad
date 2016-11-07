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

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

type Config struct {
	LookupdAddrs    string   `toml:"lookup_addrs"`
	NSQDAddrs       string   `toml:"nsqd_addrs"`
	NoticeUser      string   `toml:"notice_user"`
	NoticePasswd    string   `toml:"noitce_password"`
	SmsAddr         string   `toml:"sms_addr"`
	EmailAddr       string   `toml:"email_addr"`
	MaxChannelDepth int64    `toml:"max_channel_depth"`
	LookupInterval  duration `toml:"lookup_interval"`
	PhoneNumbers    string   `toml:"phone_numbers"`
	Emails          string   `toml:"emails"`

	MysqlUsername string `toml:"mysql_username"`
	MysqlPassword string `toml:"mysql_password"`
	MysqlHost     string `toml:"mysql_host"`
	MysqlPort     int    `toml:"mysql_port"`
	MysqlDBName   string `toml:"mysql_db_name"`
}

var Conf Config
var config string
var buildstamp = "no timestamp set"
var githash = "no githash set"

func init() {
	flag.StringVar(&Conf.LookupdAddrs, "lookupdAddrs", "127.0.0.1:4161", "nsq lookupd addrs")
	flag.StringVar(&Conf.NSQDAddrs, "nsqdAddrs", "", "nsqd addrs")
	flag.StringVar(&Conf.NoticeUser, "alert-user", "", "alert username")
	flag.StringVar(&Conf.NoticePasswd, "alert-passwd", "", "alert password")
	flag.StringVar(&Conf.SmsAddr, "sms-addr", "", "sms alert addr")
	flag.StringVar(&Conf.EmailAddr, "email-addr", "", "email alert addr")
	flag.Int64Var(&Conf.MaxChannelDepth, "max-channel-depth", 10000, "max channel depth to alert")
	flag.DurationVar(&Conf.LookupInterval.Duration, "lookup-interval", time.Minute, "lookup nsqd stats interval")
	flag.StringVar(&Conf.PhoneNumbers, "alert-phone-numbers", "", "phone numbers to alert, number1,number2")
	flag.StringVar(&Conf.Emails, "alert-emails", "", "emails to alert, email1,email2")

	flag.StringVar(&Conf.MysqlUsername, "mysql-username", "", "mysql username for byroad")
	flag.StringVar(&Conf.MysqlPassword, "mysql-password", "", "mysql password for byroad")
	flag.StringVar(&Conf.MysqlHost, "mysql-host", "localhost", "mysql host for byroad")
	flag.IntVar(&Conf.MysqlPort, "mysql-port", 3306, "mysql port for byroad")
	flag.StringVar(&Conf.MysqlDBName, "mysql-dbname", "byroad", "mysql db name")

	flag.StringVar(&config, "config", "", "config file for nsq monitor")

	info := flag.Bool("info", false, "Print build info & exit")
	flag.Parse()
	if *info {
		fmt.Println("build stamp: ", buildstamp)
		fmt.Println("githash: ", githash)
		os.Exit(0)
	}
}

func main() {
	if config != "" {
		_, err := toml.DecodeFile(config, &Conf)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("config: %+v", Conf)
	}
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8&parseTime=true", Conf.MysqlUsername, Conf.MysqlPassword, Conf.MysqlHost, Conf.MysqlPort, Conf.MysqlDBName)
	confdb, err := sqlx.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	model.Init(confdb)
	var lookups, nsqs []string
	if Conf.LookupdAddrs != "" {
		lookups = strings.Split(Conf.LookupdAddrs, ",")
	}
	if Conf.NSQDAddrs != "" {
		nsqs = strings.Split(Conf.NSQDAddrs, ",")
	}
	var phoneNumbers, emails []string
	if Conf.PhoneNumbers != "" {
		phoneNumbers = strings.Split(Conf.PhoneNumbers, ",")
	}
	if Conf.Emails != "" {
		emails = strings.Split(Conf.Emails, ",")
	}
	config := &MonitorConfig{
		MaxChannelDepth: Conf.MaxChannelDepth,
		LookupInterval:  Conf.LookupInterval.Duration,
		PhoneNumbers:    phoneNumbers,
		Emails:          emails,
	}
	noticeConfig := &notice.Config{
		User:      Conf.NoticeUser,
		Password:  Conf.NoticePasswd,
		SmsAddr:   Conf.SmsAddr,
		EmailAddr: Conf.EmailAddr,
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
