package main

import (
	"fmt"
	"mysql_byroad/model"

	"strconv"

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

func SaveBinlogInfo() (int64, error) {
	SaveConfig("last_file_name", binlogInfo.Filename, "")
	SaveConfig("last_position", fmt.Sprintf("%d", binlogInfo.Position), "")
	return 0, nil
}

func GetBinlogInfo() (*model.BinlogInfo, error) {
	binfo := &model.BinlogInfo{}
	binfo.Filename = GetConfig("last_file_name")
	pos, _ := strconv.Atoi(GetConfig("last_position"))
	binfo.Position = uint32(pos)
	return binfo, nil
}
