package main

import (
	"fmt"
	"mysql_byroad/model"

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
