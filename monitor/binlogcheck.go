package main

import (
	"log"
	"mysql_byroad/model"
	"time"
)

type BinlogChecker struct {
	Addrs             map[string]string
	dispatcherManager *DispatcherManager
}

func NewBinlogChecker(dm *DispatcherManager) *BinlogChecker {
	checker := &BinlogChecker{
		Addrs:             make(map[string]string),
		dispatcherManager: dm,
	}
	return checker
}

func (this *BinlogChecker) AddDispatcher(name, addr string) {
	this.Addrs[name] = addr
}

func (this *BinlogChecker) GetMasterStatus() map[string]*model.BinlogInfo {
	infoMap := make(map[string]*model.BinlogInfo, 10)
	for name, _ := range this.Addrs {
		status, _ := this.dispatcherManager.GetMasterStatus(name)
		if status != nil {
			infoMap[name] = status
		}
	}
	return infoMap
}

func (this *BinlogChecker) GetCurrentBinlogInfo() map[string]*model.BinlogInfo {
	infoMap := make(map[string]*model.BinlogInfo, 10)
	for name, _ := range this.Addrs {
		status, _ := this.dispatcherManager.GetCurrentBinlogInfo(name)
		if status != nil {
			infoMap[name] = status
		}
	}
	return infoMap
}

func (this *BinlogChecker) Run() {
	lastBinlogInfoMap := this.GetCurrentBinlogInfo()
	for {
		time.Sleep(time.Second * 60)
		currentInfoMap := this.GetCurrentBinlogInfo()
		for name, info := range currentInfoMap {
			lastInfo, ok := lastBinlogInfoMap[name]
			if ok {
				if lastInfo.Filename == info.Filename && lastInfo.Position == info.Position {
					log.Printf("binlog check error: %s-> %s:%d\n", name, info.Filename, info.Position)
				}
			}
		}
		lastBinlogInfoMap = currentInfoMap
	}
}
