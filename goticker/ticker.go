package goticker

import (
	"sync"
	"time"
)

type Ticker struct {
	fn       func(arg interface{})
	ch       chan bool
	wg       sync.WaitGroup
	interval int
}

func New(interval int, fn func(arg interface{})) *Ticker {
	return &Ticker{
		fn:       fn,
		ch:       make(chan bool, 1),
		interval: interval,
	}
}

func (this *Ticker) Tick(arg interface{}) {
	this.wg.Add(1)
	ticker := time.NewTicker(time.Second * time.Duration(this.interval))
	for {
		select {
		case <-ticker.C:
			this.fn(arg)
		case <-this.ch:
			ticker.Stop()
			this.wg.Done()
			return
		}
	}
}

func (this *Ticker) Stop() {
	this.ch <- true
	this.wg.Wait()
}
