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
	Logfile         string        `toml:"logfile"`
	RPCPingInterval duration      `toml:"rpc_ping_interval"`
	MonitorConf     MonitorConf   `toml:"monitor"`
	RPCServerConf   RPCServerConf `toml:"rpc_server"`
	NSQConf         NSQConf       `toml:"nsq"`
	LogLevel        string        `toml:"loglevel"`
	KafkaConf       KafkaConfig   `toml:"kafka"`
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
	LookupInterval   duration `toml:"lookup_interval"`
}

type KafkaConfig struct {
	Hosts                   []string `toml:"hosts"`
	OffsetProcessingTimeout duration `toml:"offset_processing_timeout"`
	OffsetResetOffsets      bool     `toml:"offset_reset_offsets"`
	ZkAddrs                 []string `toml:"zk_addrs"`
	ZKPrefix                string   `toml:"zk_prefix"`
}

type MysqlInstanceConfig struct {
	Name     string
	Host     string
	Port     uint16
	Username string
	Password string
	Include  []string
	Exclude  []string
	Interval duration
}

func InitConfig() *Config {
	config := Config{}
	configFile := ParseConfig()
	_, err := toml.DecodeFile(configFile, &config)
	if err != nil {
		panic(err)
	}
	return &config
}

var buildstamp = "no timestamp set"
var githash = "no githash set"

func ParseConfig() string {
	filename := flag.String("c", "dispatcher.toml", "config file path")
	info := flag.Bool("info", false, "Print build info & exit")
	flag.Parse()
	if *info {
		fmt.Println("build stamp: ", buildstamp)
		fmt.Println("githash: ", githash)
		os.Exit(0)
	}
	return *filename
}
