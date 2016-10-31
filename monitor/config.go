package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/BurntSushi/toml"
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
	Debug                   bool
	RPCClientLookupInterval duration      `toml:"rpcclient_lookup_interval"`
	NSQLookupdAddress       []string      `toml:"nsqlookupd_http_address"`
	Logfile                 string        `toml:"logfile"`
	MysqlConf               MysqlConf     `toml:"mysql"`
	RPCServerConf           RPCServerConf `toml:"rpcserver"`
	WebConfig               WebConfig     `toml:"web"`
	LogLevel                string        `toml:"loglevel"`
	AlertConfig             AlertConfig   `toml:"alert"`
}
type MysqlConf struct {
	Host     string
	Port     uint16
	Username string
	Password string
	DBName   string `toml:"dbname"`
}

type RPCServerConf struct {
	Host string
	Port int
}

type WebConfig struct {
	Host      string
	Port      int
	AuthURL   string `toml:"auth_url"`
	AppKey    string `toml:"appkey"`
	AppName   string `toml:"appname"`
	AliasName string `toml:"aliasname"`
}

type AlertConfig struct {
	User              string
	Password          string
	SmsAddr           string   `toml:"sms_addr"`
	EmailAddr         string   `toml:"email_addr"`
	PhoneNumbers      string   `toml:"phone_numbers"`
	Emails            string   `toml:"emails"`
	BinlogCheckPeriod duration `toml:"binlog_check_period"`
	BinlogPosGap      uint32   `toml:"binlog_pos_gap"`
}

var Conf Config

func init() {
	configFile := ParseConfig()
	_, err := toml.DecodeFile(configFile, &Conf)
	if err != nil {
		panic(err)
	}
}

var buildstamp = "no timestamp set"
var githash = "no githash set"

func ParseConfig() string {
	filename := flag.String("c", "monitor.toml", "config file path")
	info := flag.Bool("info", false, "Print build info & exit")
	flag.Parse()
	if *info {
		fmt.Println("build stamp: ", buildstamp)
		fmt.Println("githash: ", githash)
		os.Exit(0)
	}
	return *filename
}
