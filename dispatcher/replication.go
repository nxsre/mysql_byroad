package main

import (
	"fmt"
	"mysql_byroad/model"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"
)

type ReplicationClient struct {
	Name           string
	ServerId       uint32
	Host           string
	Port           uint16
	Username       string
	Password       string
	BinlogFilename string
	BinlogPosition uint32
	handler        []EventHandler
	running        bool
	StopChan       chan bool
	confdb         *ConfigDB

	binlogInfo    *model.BinlogInfo
	columnManager *ColumnManager
}

/*
binlog replication实例，通过模拟mysql的slave，得到binlog信息，将binlog event发送到其所有的handler进行处理
*/
func NewReplicationClient(ctx context.Context) *ReplicationClient {
	conf := ctx.Value("dispatcher").(*Dispatcher).Config.MysqlConf
	replicationClient := &ReplicationClient{
		Name:           conf.Name,
		ServerId:       conf.ServerId,
		Host:           conf.Host,
		Port:           conf.Port,
		Username:       conf.Username,
		Password:       conf.Password,
		BinlogFilename: conf.BinlogFilename,
		BinlogPosition: conf.BinlogPosition,
		StopChan:       make(chan bool, 1),
	}
	confdb, err := NewConfigDB()
	if err != nil {
		log.Panic(err)
	}
	replicationClient.confdb = confdb
	binlogInfo := &model.BinlogInfo{}
	replicationClient.binlogInfo = binlogInfo
	columnManager := NewColumnManager(ctx)
	replicationClient.columnManager = columnManager
	return replicationClient
}

type EventHandler interface {
	HandleEvent(evt *replication.BinlogEvent)
}

func (rep *ReplicationClient) Start() {
	rep.running = true
	go startReplication(rep)
	go rep.BinlogTick()
}

func (rep *ReplicationClient) AddHandler(handler EventHandler) {
	rep.handler = append(rep.handler, handler)
}

/*
生成mysql slave的实例，其中binlog的位置信息首先从配置文件中读取，如果不存在，则使用本地数据库记录的位置，
如果本地数据库中位置信息不存在，则使用 'SHOW MASTER STATUS' 获得当前的binlog信息
*/
func startReplication(rep *ReplicationClient) {
	defer func() {
		if err := recover(); err != nil {
			log.Panicf("%v", err)
			rep.StopChan <- true
		}
	}()
	syncer := replication.NewBinlogSyncer(rep.ServerId, "mysql")
	err := syncer.RegisterSlave(rep.Host, rep.Port, rep.Username, rep.Password)
	if err != nil {
		log.Panicf("start replication on %s:%d %s", rep.Host, rep.Port, err.Error())
	}
	filename := rep.BinlogFilename
	pos := rep.BinlogPosition
	log.Debugf("config filename %s, pos %d", filename, pos)
	if filename == "" || pos == 0 {
		binfo, err := rep.confdb.GetBinlogInfo(rep.Name)
		if err != nil {
			log.Errorf(err.Error())
		}
		filename = binfo.Filename
		pos = binfo.Position
		log.Debugf("config db filename %s, pos %d", filename, pos)
		if filename == "" || pos == 0 {
			addr := fmt.Sprintf("%s:%d", rep.Host, rep.Port)
			c, err := client.Connect(addr, rep.Username, rep.Password, "")
			if err != nil {
				log.Errorf("start replication on %s:%d %s", rep.Host, rep.Port, err.Error())
			}
			rr, err := c.Execute("SHOW MASTER STATUS")
			if err != nil {
				log.Errorf("start replication on %s:%d %s", rep.Host, rep.Port, err.Error())
			}
			filename, _ = rr.GetString(0, 0)
			position, _ := rr.GetInt(0, 1)
			pos = uint32(position)
			c.Close()
		}
	}
	streamer, err := syncer.StartSync(mysql.Position{filename, pos})
	if err != nil {
		log.Fatalf("start replication on %s:%d %s", rep.Host, rep.Port, err.Error())
	}
	log.Infof("start replication client on %s:%d at %s, %d", rep.Host, rep.Port, filename, pos)
	timeout := time.Second
	for rep.running {
		ev, err := streamer.GetEventTimeout(timeout)
		if err != nil {
			if err == replication.ErrGetEventTimeout {
				continue
			} else {
				log.Error(err.Error())
				continue
			}
		}
		for _, handler := range rep.handler {
			handler.HandleEvent(ev)
		}
	}
	rep.StopChan <- true
	log.Infof("stop replication client on %s:%d", rep.Host, rep.Port)
}

func (rep *ReplicationClient) Stop() {
	rep.running = false
}

func (rep *ReplicationClient) SaveBinlog() {
	rep.confdb.SaveBinlogInfo(rep.Name, rep.binlogInfo)
}

/*
定时将binlog的信息写入本地数据库文件，防止意外丢失
*/
func (rep *ReplicationClient) BinlogTick() {
	ticker := time.NewTicker(Conf.BinlogInterval.Duration)
	for {
		select {
		case <-ticker.C:
			rep.SaveBinlog()
		}
	}
}
