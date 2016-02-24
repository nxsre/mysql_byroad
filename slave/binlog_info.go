package slave

import (
	"database/sql"
	"strconv"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

/*
binlog信息结构，包括binlog文件名和binlog位置
*/
type BinlogInfo struct {
	Filename string
	Position uint32
	ch       chan bool
	wg       sync.WaitGroup
}

func createConfigTable(confdb *sqlx.DB) {
	s := "CREATE TABLE IF NOT EXISTS `config` ( " +
		"`id` INTEGER PRIMARY KEY AUTOINCREMENT," +
		"`key` varchar(120) NOT NULL," +
		"`value` varchar(120) NOT NULL," +
		"`description` varchar(120)" +
		")"
	confdb.MustExec(s)
}

func NewBinlogInfo() *BinlogInfo {
	return &BinlogInfo{
		ch: make(chan bool, 1),
	}
}

/*
从数据库中读取binlog信息
*/
func (this *BinlogInfo) Get(confdb *sqlx.DB) error {
	var err error
	this.Filename, err = getConfig(confdb, "last_file_name")
	sysLogger.LogErr(err)
	posStr, err := getConfig(confdb, "last_position")
	pos, err := strconv.ParseUint(posStr, 10, 32)
	sysLogger.LogErr(err)
	this.Position = uint32(pos)
	return err
}

/*
将binlog信息写到数据库中
*/
func (this *BinlogInfo) Set(confdb *sqlx.DB) error {
	var err error
	_, err = setConfig(confdb, "last_file_name", this.Filename, "当前binlog文件名")
	sysLogger.LogErr(err)
	posStr := strconv.FormatUint(uint64(this.Position), 10)
	_, err = setConfig(confdb, "last_position", posStr, "当前binlog位置")
	return err
}

/*
定时将binlog信息写入数据库
*/
func (this *BinlogInfo) HandleUpdate(interval int) {
	tick := time.NewTicker(time.Second * time.Duration(interval))
	this.wg.Add(1)
	go func() {
		for {
			select {
			case <-tick.C:
				this.Set(confdb)
			case <-this.ch:
				this.wg.Done()
				return
			}
		}
	}()
}

/*
停止定时写入，退出系统时使用
*/
func (this *BinlogInfo) StopHandleUpdate() {
	this.ch <- true
	this.wg.Wait()
}

func setConfig(confdb *sqlx.DB, key, value, desc string) (int64, error) {
	var cnt int64
	err := confdb.Get(&cnt, "SELECT COUNT(*) FROM config WHERE key=?", key)
	sysLogger.LogErr(err)
	var res sql.Result
	if cnt == 0 {
		res, err = confdb.Exec("INSERT INTO config(key, value, description) VALUES(?, ?, ?)", key, value, desc)
		sysLogger.LogErr(err)
		return res.LastInsertId()
	} else {
		res, err = confdb.Exec("UPDATE config SET value=?, description=? WHERE key=?", value, desc, key)
		sysLogger.LogErr(err)
		return res.RowsAffected()
	}
}

func getConfig(confdb *sqlx.DB, key string) (string, error) {
	var value string
	err := confdb.Get(&value, "SELECT value FROM config WHERE key=?", key)
	return value, err
}
