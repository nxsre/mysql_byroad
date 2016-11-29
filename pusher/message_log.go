package main

import (
	"bufio"
	"encoding/json"
	"mysql_byroad/model"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type FilePayload struct {
	Time    time.Time
	Ret     string
	Error   string
	Message *model.NotifyEvent
}

type LogFile struct {
	pathname string
	filename string
	fp       *os.File
	reader   *bufio.Reader
	writer   *bufio.Writer
	lock     sync.RWMutex
}

func NewLogFile(pathname string) (*LogFile, error) {
	logfile := LogFile{
		pathname: pathname,
	}
	return &logfile, nil
}

func (this *LogFile) WriteLine(line []byte) error {
	var err error
	err = this.checkFile()
	if err != nil {
		return err
	}
	this.lock.Lock()
	defer this.lock.Unlock()
	_, err = this.writer.Write(line)
	if err != nil {
		return err
	}
	err = this.writer.WriteByte('\n')
	if err != nil {
		return err
	}
	err = this.writer.Flush()
	return err
}

func (this *LogFile) WritePayload(ret string, errmsg string, evt *model.NotifyEvent) error {
	payload := &FilePayload{
		Time:    time.Now(),
		Ret:     ret,
		Error:   errmsg,
		Message: evt,
	}
	buf, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	err = this.WriteLine(buf)
	return err
}

func (this *LogFile) Close() error {
	this.lock.Lock()
	defer this.lock.Unlock()
	return this.fp.Close()
}

func (this *LogFile) checkFile() error {
	var err error
	this.lock.Lock()
	defer this.lock.Unlock()
	filename := time.Now().Format("2006010215") + ".log"
	logname := filepath.Join(this.pathname, filename)
	if !fileExists(logname) {
		if this.fp != nil {
			err = this.fp.Close()
			this.fp = nil
			if err != nil {
				return err
			}
		}
		this.fp, err = os.Create(logname)
		if err != nil {
			return err
		}
		this.reader = bufio.NewReader(this.fp)
		this.writer = bufio.NewWriter(this.fp)
	} else {
		if this.fp == nil {
			this.fp, err = os.OpenFile(logname, os.O_WRONLY|os.O_APPEND, 0666)
			if err != nil {
				return err
			}
			this.reader = bufio.NewReader(this.fp)
			this.writer = bufio.NewWriter(this.fp)
		}
	}
	return err
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}
