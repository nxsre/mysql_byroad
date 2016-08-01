package main

import (
	"fmt"
	"mysql_byroad/model"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/siddontang/go-mysql/client"
)

func GetMasterStatus(conf MysqlConf) (*model.BinlogInfo, error) {
	addr := fmt.Sprintf("%s:%d", conf.Host, conf.Port)
	c, err := client.Connect(addr, conf.Username, conf.Password, "")
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

func (confdb *ConfigDB) SaveBinlogInfo(desc string, binlogInfo *model.BinlogInfo) (int64, error) {
	confdb.SaveConfig("last_file_name", binlogInfo.Filename, desc)
	confdb.SaveConfig("last_position", fmt.Sprintf("%d", binlogInfo.Position), desc)
	return 0, nil
}

func (confdb *ConfigDB) GetBinlogInfo(desc string) (*model.BinlogInfo, error) {
	var err error
	binfo := &model.BinlogInfo{}
	binfo.Filename, err = confdb.GetConfig("last_file_name", desc)
	if err != nil {
		return binfo, err
	}
	pos, err := confdb.GetConfig("last_position", desc)
	if err != nil {
		return binfo, err
	}
	pos32, err := strconv.Atoi(pos)
	if err != nil {
		return binfo, err
	}
	binfo.Position = uint32(pos32)
	return binfo, nil
}
