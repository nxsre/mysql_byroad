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
	RPCPingInterval     duration      `toml:"rpc_ping_interval"`
	Logfile             string        `toml:"logfile"`
	MonitorConf         MonitorConf   `toml:"monitor"`
	RPCServerConf       RPCServerConf `toml:"rpc_server"`
	NSQConf             NSQConf       `toml:"nsq"`
	MysqlConf           MysqlConf     `toml:"mysql"`
	MaxIdleConnsPerHost int           `toml:"max_idle_conns_per_host"`
	LogLevel            string        `toml:"loglevel"`
}

type MysqlConf struct {
	Host     string
	Port     uint16
	Username string
	Password string
	DBName   string `toml:"dbname"`
}

type MonitorConf struct {
	Host    string
	RpcPort int `toml:"rpc_port"`
}

type RPCServerConf struct {
	Host string
	Port int
	Desc string
}

type NSQConf struct {
	LookupdHttpAddrs []string `toml:"lookupd_http_address"`
	NsqdAddrs        []string `toml:"nsqd_tcp_address"`
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
	filename := flag.String("c", "pusher.toml", "config file path")
	flag.Parse()
	return *filename
}
