package slave

import "mysql_byroad/goticker"

type TickerManager struct {
	tickers []*goticker.Ticker
}

func NewTickerManager() *TickerManager {
	return &TickerManager{
		tickers: make([]*goticker.Ticker, 0, 5),
	}
}

func (this *TickerManager) Add(ticker *goticker.Ticker) {
	this.tickers = append(this.tickers, ticker)
}

func (this *TickerManager) StopAll() {
	for _, ticker := range this.tickers {
		ticker.Stop()
	}
}

func (this *TickerManager) Init() {
	interval := configer.GetSys().UpdateDuration
	taskStatisticTicker := goticker.New(interval, taskStatistics.Tick)
	this.Add(taskStatisticTicker)
	go taskStatisticTicker.Tick(nil)
	binlogTicker := goticker.New(interval, binlogInfo.Tick)
	this.Add(binlogTicker)
	go binlogTicker.Tick(nil)
	binlogStatisticTicker := goticker.New(interval, binlogStatistics.Tick)
	this.Add(binlogStatisticTicker)
	go binlogStatisticTicker.Tick(nil)
}
