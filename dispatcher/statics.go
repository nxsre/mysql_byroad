package main

import (
	"sync/atomic"

	"github.com/jmoiron/sqlx"
)

type BinlogStatic struct {
	Schema string
	Table  string
	Event  string
	Count  uint64
}

type BinlogStatics struct {
	Statics []*BinlogStatic
}

func (this *BinlogStatics) IncStatic(schema, table, event string) {
	for _, si := range this.Statics {
		if si.Schema == schema && si.Table == table && si.Event == event {
			atomic.AddUint64(&si.Count, 1)
			return
		}
	}
	info := &BinlogStatic{schema, table, event, 1}
	this.Statics = append(this.Statics, info)
}

func (this *BinlogStatics) Save(confdb *sqlx.DB) {

}

func (this *BinlogStatics) Tick(arg interface{}) {
	confdb := arg.(*sqlx.DB)
	this.Save(confdb)
}
