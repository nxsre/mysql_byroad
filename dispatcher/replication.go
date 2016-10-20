package main

import (
	"fmt"
	"mysql_byroad/model"
	"mysql_byroad/mysql_schema"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"
)

type ReplicationClient struct {
	Name        string
	ServerId    uint32
	Host        string
	Port        uint16
	Username    string
	Password    string
	handler     []EventHandler
	running     bool
	StopChan    chan bool
	confdb      *ConfigDB
	restartChan chan error
	syncer      *replication.BinlogSyncer

	binlogInfo         *model.BinlogInfo
	columnManager      *schema.ColumnManager
	saveBinlogInterval time.Duration
	timeoutToReconnect time.Duration
}

/*
binlog replication实例，通过模拟mysql的slave，得到binlog信息，将binlog event发送到其所有的handler进行处理
*/
func NewReplicationClient(ctx context.Context) *ReplicationClient {
	conf := ctx.Value("dispatcher").(*Dispatcher).Config
	myconf := conf.MysqlConf
	replicationClient := &ReplicationClient{
		Name:               myconf.Name,
		ServerId:           myconf.ServerId,
		Host:               myconf.Host,
		Port:               myconf.Port,
		Username:           myconf.Username,
		Password:           myconf.Password,
		StopChan:           make(chan bool, 1),
		restartChan:        make(chan error, 1),
		saveBinlogInterval: conf.BinlogInterval.Duration,
		binlogInfo: &model.BinlogInfo{
			Filename: myconf.BinlogFilename,
			Position: myconf.BinlogPosition,
		},
		timeoutToReconnect: myconf.TimeoutToReconnect.Duration,
	}
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8&parseTime=true",
		conf.DBConfig.Username, conf.DBConfig.Password, conf.DBConfig.Host, conf.DBConfig.Port,
		conf.DBConfig.DBName)
	confdb, err := NewConfigDB(dsn)
	if err != nil {
		log.Panic(err)
	}
	replicationClient.confdb = confdb
	binlogInfo := &model.BinlogInfo{}
	replicationClient.binlogInfo = binlogInfo
	columnManager, err := schema.NewColumnManager([]*schema.MysqlConfig{
		&schema.MysqlConfig{
			Host:     conf.MysqlConf.Host,
			Port:     conf.MysqlConf.Port,
			Username: conf.MysqlConf.Username,
			Password: conf.MysqlConf.Password,
			Exclude:  conf.MysqlConf.Exclude,
			Include:  conf.MysqlConf.Include,
			Interval: conf.MysqlConf.Interval.Duration,
			Name:     conf.MysqlConf.Name,
		},
	})
	if err != nil {
		log.Fatalf("new column manager error: %s", err.Error())
	}

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
		}
	}()
	err := rep.prepareBinlog()
	if err != nil {

	}
	log.Infof("start replication client on %s:%d, %s:%d", rep.Host, rep.Port, rep.binlogInfo.Filename, rep.binlogInfo.Position)
	rep.startBinlog()
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
	ticker := time.NewTicker(rep.saveBinlogInterval)
	for {
		select {
		case <-ticker.C:
			rep.SaveBinlog()
		}
	}
}

func (rep *ReplicationClient) prepareBinlog() error {
	filename := rep.binlogInfo.Filename
	pos := rep.binlogInfo.Position
	log.Debugf("config filename %s, pos %d", filename, pos)
	if filename == "" || pos == 0 {
		binfo, err := rep.confdb.GetBinlogInfo(rep.Name)
		if err != nil {
			return err
		}
		filename = binfo.Filename
		pos = binfo.Position
		log.Debugf("config db filename %s, pos %d", filename, pos)
		if filename == "" || pos == 0 {
			addr := fmt.Sprintf("%s:%d", rep.Host, rep.Port)
			c, err := client.Connect(addr, rep.Username, rep.Password, "")
			if err != nil {
				log.Errorf("start replication on %s:%d %s", rep.Host, rep.Port, err.Error())
				return err
			}
			rr, err := c.Execute("SHOW MASTER STATUS")
			if err != nil {
				log.Errorf("start replication on %s:%d %s", rep.Host, rep.Port, err.Error())
				return err
			}
			filename, _ = rr.GetString(0, 0)
			position, _ := rr.GetInt(0, 1)
			pos = uint32(position)
			c.Close()
		}
	}
	rep.binlogInfo.Filename = filename
	rep.binlogInfo.Position = pos
	return nil
}

func (rep *ReplicationClient) startBinlog() {
	stream := rep.getStreamer()
	go rep.onStream(stream)
	for {
		select {
		case err := <-rep.restartChan:
			log.Errorf("restart replication because of: %s", err.Error())
			rep.restart()
		case <-rep.StopChan:
			rep.syncer.Close()
			return
		}
	}
}

func (rep *ReplicationClient) restart() {
	rep.syncer.Close()
	stream := rep.getStreamer()
	go rep.onStream(stream)
}

func (rep *ReplicationClient) getStreamer() *replication.BinlogStreamer {
	cfg := replication.BinlogSyncerConfig{
		ServerID: rep.ServerId,
		Flavor:   "mysql",
		Host:     rep.Host,
		Port:     rep.Port,
		User:     rep.Username,
		Password: rep.Password,
	}
	syncer := replication.NewBinlogSyncer(&cfg)
	rep.syncer = syncer
	var streamer *replication.BinlogStreamer
	var err error
	for {
		streamer, err = syncer.StartSync(mysql.Position{rep.binlogInfo.Filename, rep.binlogInfo.Position})
		if err != nil {
			log.Errorf("start replication on %s:%d %s", rep.Host, rep.Port, err.Error())
			time.Sleep(time.Second)
			continue
		} else {
			break
		}
	}
	return streamer
}

func (rep *ReplicationClient) onStream(streamer *replication.BinlogStreamer) {
	timeout := rep.timeoutToReconnect
	fmt.Println(timeout.String())
	for rep.running {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		ev, err := streamer.GetEvent(ctx)
		cancel()
		if err != nil {
			if err == context.DeadlineExceeded {
				rep.restartChan <- err
				return
			} else {
				rep.restartChan <- err
				return
			}
		}
		for _, handler := range rep.handler {
			handler.HandleEvent(ev)
		}
	}
	rep.StopChan <- true
}
