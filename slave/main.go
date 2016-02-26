package slave

import (
	"fmt"
	"mysql-slave/common"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

var (
	sysLogger      *common.SysLogger
	eventLogger    *common.EventLogger
	owl            *common.OWL
	configer       *common.Configer
	queueManager   *QueueManger
	eventEnqueuer  *EventEnqueuer
	columnManager  *ColumnManager
	routineManager *RoutineManager
)

var (
	eventDoneChan = make(chan bool)
	signalChan    = make(chan os.Signal, 1)
	cleanUpDone   = make(chan bool)
	startChan     = make(chan bool, 1)
)

var (
	running             bool
	updateConfigRunning bool
	startTime           time.Time
	confdb              *sqlx.DB
	binlogInfo          *BinlogInfo
	rpcserver           *ByRoad
	totalStatic         Static
	binlogStatics       BinlogStatics
	taskStatic          TaskStatic
)

func StartSlave() {
	var err error
	runtime.GOMAXPROCS(runtime.NumCPU())
	//writePid()
	parser := new(common.Parser)
	//解析命令行传递的配置文件信息，默认为config.conf
	configFile := parser.ParseConfig()

	running = true
	updateConfigRunning = true

	//解析配置文件信息
	configer, err = common.NewConfiger(configFile)
	if err != nil {
		panic(err.Error())
	}

	//系统日志初始化
	sysfile := configer.GetString("log", "sys_log_path")
	sysLogger, err = common.NewSysLogger("", sysfile)
	if err != nil {
		panic(err.Error())
	}

	//消息日志初始化
	evtdir := configer.GetString("log", "err_log_path")
	eventLogger, err = common.NewEventLogger(evtdir)
	if err != nil {
		panic(err.Error())
	}

	//OWL日志初始化
	owl = common.NewOWL(configer.GetString("OWL", "path", "/tmp"), configer.GetOWL())
	owl.LogThisException("test exception")
	httpClient = NewHttpClient()
	confdb, err = sqlx.Open("sqlite3", "./config.db")
	if err != nil {
		panic(err)
	}
	//初始化数据库，读取数据库信息
	initNotifyAPIDB(confdb)
	queueManager = NewQueueManager(configer.GetRedis())
	columnManager = NewColumnManager(configer.GetMysql()) //读取数据库的information_schema表，获得所有的列信息
	routineManager = NewRoutineManager()
	routineManager.InitTaskRoutines()
	eventEnqueuer = NewEventEnqueue()

	//定时将binlog文件的信息写到数据库，下次启动时将从该位置继续处理
	binlogInfo = NewBinlogInfo()
	binlogInfo.HandleUpdate(configer.GetInt("system", "config_update_duration", 5))
	rpcConfiger := configer.GetRPCServer()
	rpcserver = NewRPCServer("tcp", rpcConfiger.Schema)
	rpcserver.start()
	startChan <- true
	startTime = time.Now()
	registerSignal()
	startReplication()
	//注册系统退出时的处理函数
	<-cleanUpDone
}

func writePid() {
	pid := os.Getpid()
	file, err := os.OpenFile("/tmp/slave.pid", os.O_CREATE|os.O_RDWR, 0644)
	panic(err)
	pidStr := strconv.Itoa(pid)
	_, err = file.WriteString(pidStr)
	panic(err)
	file.Close()
}

/*
   连接数据库，从binlog中获得相关信息
*/
func startReplication() {
	sysConfiger := configer.GetSys()
	syncer := replication.NewBinlogSyncer(sysConfiger.ServerID, "mysql")
	mc := configer.GetMysql()
	err := syncer.RegisterSlave(mc.Host, uint16(mc.Port), mc.Username, mc.Password)
	sysLogger.LogErr(err)
	binlogInfo.Get(confdb)
	if binlogInfo.Filename == "" || binlogInfo.Position == 0 {
		addr := fmt.Sprintf("%s:%d", mc.Host, mc.Port)
		c, err := client.Connect(addr, mc.Username, mc.Password, "")
		sysLogger.LogErr(err)
		rr, err := c.Execute("SHOW MASTER STATUS")
		sysLogger.LogErr(err)
		binlogInfo.Filename, _ = rr.GetString(0, 0)
		position, _ := rr.GetInt(0, 1)
		binlogInfo.Position = uint32(position)
		c.Close()
	}
	streamer, err := syncer.StartSync(mysql.Position{binlogInfo.Filename, binlogInfo.Position})
	sysLogger.LogErr(err)
	timeout := time.Second
	for running {
		ev, err := streamer.GetEventTimeout(timeout)
		if err != nil {
			if err == replication.ErrGetEventTimeout {
				continue
			} else {
				sysLogger.LogErr(err)
				os.Exit(2)
			}
		}
		switch e := ev.Event.(type) {
		case *replication.RowsEvent:
			switch ev.Header.EventType {
			case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				handleWriteEvent(e)
			case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				handleDeleteEvent(e)
			case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				handleUpdateEvent(e)
			default:
				//return errors.Errorf("%s not supported now", ev.Header.EventType)
			}
			binlogInfo.Position = ev.Header.LogPos

		case *replication.RotateEvent:
			binlogInfo.Filename = string(e.NextLogName)
			binlogInfo.Position = uint32(e.Position)
		}
	}
	eventDoneChan <- true
}

/*
   注册程序退出的时的处理
*/
func registerSignal() {
	go func() {
		signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1)
		for sig := range signalChan {
			switch sig {
			case syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
				cleanUp()
			case syscall.SIGUSR1:
				//reload()
			}
		}
	}()
}

/*
   程序退出时，将binlog的信息写入数据库，关闭连接
*/
func cleanUp() {
	running = false
	updateConfigRunning = false
	sysLogger.Log("stop")
	<-eventDoneChan
	sysLogger.Log("event chan done")
	binlogInfo.Set(confdb)
	binlogInfo.StopHandleUpdate()
	sysLogger.Log("update config done")
	routineManager.Clean()
	queueManager.Clean()
	confdb.Close()
	rpcserver.deregister(configer.GetString("rpc", "schema"))
	//showStatics()
	//fmt.Println(fmt.Sprintf("insert %d, delete %d, update %d", insertCount, deleteCount, updateCount))
	//sysLogger.Log(fmt.Sprintf("insert %d, delete %d, update %d", insertCount, deleteCount, updateCount))
	cleanUpDone <- true
}
