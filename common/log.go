package common

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"time"
)

type EventLogger struct {
	dir string
}

type SysLogger struct {
	dir      string
	filename string
	logger   *log.Logger
}

type OWL struct {
	filepath string
	filename string
	logger   *log.Logger
	config   *OWLConfig
}

/*
推送超过失败次数的消息日志，文件名为20150101格式
*/
func NewEventLogger(dir string) (*EventLogger, error) {
	logger := EventLogger{
		dir: dir,
	}
	if !FileExist(dir) {
		err := os.MkdirAll(dir, 0777)
		if err != nil {
			return &logger, err
		}
	}

	return &logger, nil
}

type SendError struct {
	Time   time.Time    `json:"time"`
	Event  *NotifyEvent `json:"event"`
	Reason string       `json:"reason"`
}

func (this *EventLogger) Log(evt *NotifyEvent, reason error) error {
	filename := getTodayName(this.dir)
	logfile, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0664)
	if err != nil {
		return err
	}
	se := new(SendError)
	se.Event = evt
	se.Time = time.Now()
	if reason != nil {
		se.Reason = reason.Error()
	}
	msgStr, _ := json.Marshal(se)
	logfile.WriteString(string(msgStr))
	//logfile.WriteString("\n")
	logfile.Close()
	return nil
}

/*
系统日志，文件名通过配置文件中的sys_log_path配置
*/
func NewSysLogger(dir, filename string) (*SysLogger, error) {
	logger := SysLogger{
		dir:      dir,
		filename: filename,
	}
	fullname := filepath.Join(dir, filename)
	dirname := filepath.Dir(filename)
	if !FileExist(dirname) {
		err := os.MkdirAll(dirname, 0777)
		if err != nil {
			return nil, err
		}
	}
	file, err := os.OpenFile(fullname, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0664)
	if err != nil {
		return nil, err
	}
	logger.logger = log.New(file, "", log.LstdFlags)
	return &logger, nil
}

func (this *SysLogger) Log(msg string) {
	this.logger.Println(msg)
}

func (this *SysLogger) LogErr(err error) {
	if err != nil {
		this.logger.Println(err.Error())
	}
}

func (this *SysLogger) PanicErr(err error) {
	if err != nil {
		this.logger.Panic(err)
	}
}

func NewOWL(filepath string, config *OWLConfig) *OWL {
	owl := OWL{
		filepath: filepath,
		config:   config,
	}
	if !FileExist(filepath) {
		err := os.MkdirAll(filepath, 0777)
		if err != nil {
			println(err)
		}
	}
	return &owl
}

func (this *OWL) LogStats(app, ip, keys, values string) {
	this.filename = getTodayName("")
	file := path.Join(this.filepath, app+"."+this.filename)
	f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0664)
	if err != nil {
		println(err)
	}
	defer f.Close()
	tmp := "OWL\001STATS\0010002\001%s\001%s.000\001%s\001%s\001%s\004\n"
	t := time.Now()
	ts := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second())
	logString := fmt.Sprintf(tmp, app, ts, ip, keys, values)
	f.WriteString(logString)
}

func (this *OWL) LogThisStat(keys, values string) {
	ip := this.config.App
	app := this.config.IP
	this.LogStats(app, ip, keys, values)
}

func (this *OWL) LogException(app, ip, errInfo string) {
	this.filename = getTodayName("")
	file := path.Join(this.filepath, app+"."+this.filename)
	f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0664)
	if err != nil {
		println(err)
	}
	defer f.Close()
	tmp := "OWL\001DATA\0010002\001%s\001%s.000\001%s\001Exception\001\001\001ERROR\001%s\004\n"
	t := time.Now()
	ts := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second())
	logString := fmt.Sprintf(tmp, app, ts, ip, errInfo)
	f.WriteString(logString)
}

func (this *OWL) LogThisException(errInfo string) {
	ip := this.config.App
	app := this.config.IP
	this.LogException(app, ip, errInfo)
}
