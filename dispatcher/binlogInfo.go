package main

import (
	"fmt"
	"mysql_byroad/model"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/siddontang/go-mysql/client"
)

func GetMasterStatus() (*model.BinlogInfo, error) {
	addr := fmt.Sprintf("%s:%d", Conf.MysqlConf.Host, Conf.MysqlConf.Port)
	c, err := client.Connect(addr, Conf.MysqlConf.Username, Conf.MysqlConf.Password, "")
	if err != nil {
		log.Error("get master status: ", err.Error())
		return nil, err
	}
	rr, err := c.Execute("SHOW MASTER STATUS")
	if err != nil {
		log.Error("get master status: ", err.Error())
		return nil, err
	}
	filename, _ := rr.GetString(0, 0)
	position, _ := rr.GetInt(0, 1)
	pos := uint32(position)
	c.Close()
	binfo := new(model.BinlogInfo)
	binfo.Filename = filename
	binfo.Position = pos
	return binfo, nil
}

func (confdb *ConfigDB) SaveBinlogInfo() (int64, error) {
	confdb.SaveConfig("last_file_name", binlogInfo.Filename, "")
	confdb.SaveConfig("last_position", fmt.Sprintf("%d", binlogInfo.Position), "")
	return 0, nil
}

func (confdb *ConfigDB) GetBinlogInfo() (*model.BinlogInfo, error) {
	var err error
	binfo := &model.BinlogInfo{}
	binfo.Filename, err = confdb.GetConfig("last_file_name")
	if err != nil {
		return nil, err
	}
	pos, err := confdb.GetConfig("last_position")
	if err != nil {
		return nil, err
	}
	pos32, err := strconv.Atoi(pos)
	if err != nil {
		return nil, err
	}
	binfo.Position = uint32(pos32)
	return binfo, nil
}

func binlogTicker() {
	ticker := time.NewTicker(Conf.BinlogInterval.Duration)
	for {
		select {
		case <-ticker.C:
			confdb.SaveBinlogInfo()
		}
	}
}
