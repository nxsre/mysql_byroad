package gorpool

import (
	"container/list"
	"sync"
)

type routine struct {
	name  string
	index int
	ch    chan bool
	f     func(name string)
	wg    *sync.WaitGroup
}

type RoutinePool struct {
	name     string
	routines *list.List
	fn       func(name string)
	wg       sync.WaitGroup
}

func (this *routine) do() {
	for {
		select {
		case b := <-this.ch:
			if b {
				continue
			} else {
				this.wg.Done()
				return
			}
		default:
			this.f(this.name)
		}
	}
}

func (this *routine) start() {
	go this.do()
}

func (this *routine) stop() {
	this.ch <- false
}

func NewPool(size int, fn func(name string), name string) *RoutinePool {
	pool := RoutinePool{
		name:     name,
		routines: list.New(),
		fn:       fn,
	}
	for i := 0; i < size; i++ {
		r := newRoutine(i, name, fn)
		r.wg = &pool.wg
		r.wg.Add(1)
		r.start()
		pool.routines.PushBack(r)
	}
	return &pool
}

func (this *RoutinePool) GetName() string {
	return this.name
}

func (this *RoutinePool) Incr(n int) {
	routines := this.routines
	for i := 0; i < n; i++ {
		r := newRoutine(this.Size(), this.name, this.fn)
		r.wg = &this.wg
		r.wg.Add(1)
		r.start()
		routines.PushBack(r)
	}
}

func (this *RoutinePool) Desc(n int) {
	routines := this.routines
	for i := 0; i < n; i++ {
		e := routines.Front()
		v := routines.Remove(e)
		r := v.(*routine)
		r.stop()
	}
}

func (this *RoutinePool) ChangeTo(num int) {
	if this.Size() == num {
		return
	} else if this.Size() > num {
		d := this.Size() - num
		this.Desc(d)
	} else {
		d := num - this.Size()
		this.Incr(d)
	}
}

func (this *RoutinePool) Size() int {
	return this.routines.Len()
}

func (this *RoutinePool) Wait() {
	this.wg.Wait()
}

func (this *RoutinePool) Clean() {
	for e := this.routines.Front(); e != nil; e = e.Next() {
		r := (e.Value).(*routine)
		r.stop()
	}
	this.routines.Init()
}

func newRoutine(i int, name string, do func(name string)) *routine {
	return &routine{
		name:  name,
		ch:    make(chan bool, 1),
		f:     do,
		index: i,
	}
}
