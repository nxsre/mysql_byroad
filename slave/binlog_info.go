package slave

import (
	"fmt"
	"strconv"
	"sync"
	"github.com/siddontang/go-mysql/client"
	"mysql_byroad/model"
)

var binlogConfig = &model.Config{}
/*
binlog信息结构，包括binlog文件名和binlog位置
*/
type BinlogInfo struct {
	Filename string
	Position uint32
	ch       chan bool
	wg       sync.WaitGroup
}

func NewBinlogInfo() *BinlogInfo {
	return &BinlogInfo{
		ch: make(chan bool, 1),
	}
}

/*
从数据库中读取binlog信息
*/
func (this *BinlogInfo) Get() error {
	var err error
	this.Filename, err = binlogConfig.Get("last_file_name")
	sysLogger.LogErr(err)
	posStr, err := binlogConfig.Get("last_position")
	pos, err := strconv.ParseUint(posStr, 10, 32)
	sysLogger.LogErr(err)
	this.Position = uint32(pos)
	return err
}

/*
将binlog信息写到数据库中
*/
func (this *BinlogInfo) Set() error {
	var err error
	_, err = binlogConfig.Set("last_file_name", this.Filename, "当前binlog文件名")
	sysLogger.LogErr(err)
	posStr := strconv.FormatUint(uint64(this.Position), 10)
	_, err = binlogConfig.Set("last_position", posStr, "当前binlog位置")
	return err
}

func (this *BinlogInfo) Tick(_ interface{}) {
	this.Set()
}


func GetMasterStatus() (binfo *BinlogInfo) {
	mc := configer.GetMysql()
	addr := fmt.Sprintf("%s:%d", mc.Host, mc.Port)
	c, err := client.Connect(addr, mc.Username, mc.Password, "")
	sysLogger.LogErr(err)
	rr, err := c.Execute("SHOW MASTER STATUS")
	sysLogger.LogErr(err)
	filename, _ := rr.GetString(0, 0)
	position, _ := rr.GetInt(0, 1)
	pos := uint32(position)
	c.Close()
	binfo = new(BinlogInfo)
	binfo.Filename = filename
	binfo.Position = pos
	return
}
