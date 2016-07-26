package main

import (
	"flag"
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
	Host    string
	Port    int
	AuthURL string `toml:"auth_url"`
	AppKey  string `toml:"appkey"`
	AppName string `toml:"appname"`
}

var Conf Config

func init() {
	configFile := ParseConfig()
	_, err := toml.DecodeFile(configFile, &Conf)
	if err != nil {
		panic(err)
	}
}

func ParseConfig() string {
	filename := flag.String("c", "monitor.toml", "config file path")
	flag.Parse()
	return *filename
}
