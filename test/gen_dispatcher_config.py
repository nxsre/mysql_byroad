#!/usr/bin/env python
#coding:utf-8


import argparse
from string import Template


config_template = """dispatcher config
rpc_ping_interval = "10s" #心跳时间
binlog_flush_interval = "60s" #binlog位置写入数据库的时间间隔
logfile = "/home/logs/app/byroad/dispatcher_$db_instance.log"
loglevel = "Error" # Debug Info Warn Error Fatal Panic
db_instance_names = ["$db_instance"]  #该实例的名称，可以使用多个，用来表示相应的数据库，用于区分不同实例上的任务
#mysql slave 信息
[mysql]
name = "$db_instance"
server_id = 2002
host = "$mysql_host"
port = 6006
username = "root"
password = "toor"
binlog_filename = ""
binlog_position = 0
exclude = ["information_schema", "performance_schema", "mysql"]
# include = ["test"]
interval = "60s" # interval to update information schema
reconnect = false # 超时时是否重连mysql
timeout_to_reconnect = "10s" # 接收binlog的超时时间，配合reconnect，将在超时时进行重连

# monitor的地址和rpc信息，用于获取任务信息
[monitor]
host = "127.0.0.1"
rpc_port = 1234

# 本机的rpcserver信息，用于接收monitor的rpc调用，对任务进行操作等
[rpc_server]
host = "127.0.0.1"
port = 0 #随机选择端口

[nsq]
# lookupd_http_address = ["127.0.0.1:4161"]
nsqd_tcp_address = ["127.0.0.1:4150"]
lookup_interval = "60s" # 向nsqlookupd轮询nsqd节点信息

# 保存binlog信息
[db_config]
host = "127.0.0.1"
port = 3306
username = "root"
password = "toor"
dbname = "byroad"
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser("byroad dispatcher config generator")
    parser.add_argument("--db_instance", default="localhost")
    parser.add_argument("--mysql_host", default="127.0.0.1")
    args = parser.parse_args()
    s = Template(config_template)
    print s.substitute(db_instance=args.db_instance, mysql_host=args.mysql_host)