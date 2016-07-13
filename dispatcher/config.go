package main

import "github.com/BurntSushi/toml"
import "mysql_byroad/common"

type Config struct {
	MysqlConfs    []MysqlConf   `toml:"mysql"`
	MonitorConf   MonitorConf   `toml:"monitor"`
	RPCServerConf RPCServerConf `toml:"rpc_server"`
	NSQConf       NSQConf       `toml:"nsq"`
}

type MysqlConf struct {
	ServerId       uint32 `toml:"server_id"`
	Host           string
	Port           uint16
	Username       string
	Password       string
	BinlogFilename string `toml:"binlog_filename"`
	BinlogPosition uint32 `toml:"binlog_position"`
	Exclude        []string
}

type MonitorConf struct {
	Host    string
	RpcPort int `toml:"rpc_port"`
}

type RPCServerConf struct {
	Host string
	Port int
}

type NSQConf struct {
	LookupdHttpAddrs []string `toml:"lookupd_http_address"`
	NsqdAddrs        []string `toml:"nsqd_tcp_address"`
}

var Conf Config

func init() {
	configFile := common.ParseConfig()
	_, err := toml.DecodeFile(configFile, &Conf)
	if err != nil {
		panic(err)
	}
}
