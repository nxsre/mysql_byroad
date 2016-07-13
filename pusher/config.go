package main

import "github.com/BurntSushi/toml"

type Config struct {
	MonitorConf   MonitorConf   `toml:"monitor"`
	RPCServerConf RPCServerConf `toml:"rpc_server"`
	NSQConf       NSQConf       `toml:"nsq"`
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
	_, err := toml.DecodeFile("config.toml", &Conf)
	if err != nil {
		panic(err)
	}
}
