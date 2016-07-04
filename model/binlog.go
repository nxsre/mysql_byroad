package model

import "sync/atomic"

type BinlogStatistic struct {
	Schema string
	Table  string
	Event  string
	Count  uint64
}

type BinlogStatistics struct {
	Statistics []*BinlogStatistic
}

func (this *BinlogStatistics) IncStatistic(schema, table, event string) {
	for _, si := range this.Statistics {
		if si.Schema == schema && si.Table == table && si.Event == event {
			atomic.AddUint64(&si.Count, 1)
			return
		}
	}
	info := &BinlogStatistic{schema, table, event, 1}
	this.Statistics = append(this.Statistics, info)
}

func (this *BinlogStatistics) Save() {

}

func (this *BinlogStatistics) Tick(_ interface{}) {
	this.Save()
}