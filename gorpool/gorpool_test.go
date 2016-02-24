package gorpool

import (
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	pool := NewPool(3, doSomething)
	if pool.Size() != 3 {
		t.Error("pool size is not 3")
	}
	pool.Incr(2)
	if pool.Size() != 5 {
		t.Error("pool size is not 5")
	}
	pool.Desc(3)
	if pool.Size() != 2 {
		t.Error("pool size is not 2")
	}
	go func() {
		time.Sleep(time.Second * 2)
		pool.Clean()
	}()
	pool.Wait()
}

func doSomething() {
	time.Sleep(time.Second)
}
