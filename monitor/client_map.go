package main

import (
	"sync"
	"time"
)

type RPCClientMap struct {
	cmap map[string]*RPCClient
	sync.RWMutex
}

func NewRPCClientMap() *RPCClientMap {
	cmap := make(map[string]*RPCClient, 10)
	rpcmap := RPCClientMap{
		cmap: cmap,
	}
	return &rpcmap
}

func (cmap *RPCClientMap) Get(key string) (*RPCClient, bool) {
	cmap.RLock()
	defer cmap.RUnlock()
	client, ok := cmap.cmap[key]
	return client, ok
}

func (cmap *RPCClientMap) Set(key string, client *RPCClient) {
	cmap.Lock()
	defer cmap.Unlock()
	cmap.cmap[key] = client
}

func (cmap *RPCClientMap) Delete(key string) {
	cmap.Lock()
	defer cmap.Unlock()
	delete(cmap.cmap, key)
}

func (cmap *RPCClientMap) Iter() <-chan *RPCClient {
	var ch = make(chan *RPCClient)
	go func() {
		for _, client := range cmap.cmap {
			ch <- client
		}
		close(ch)
	}()
	return ch
}

func (cmap *RPCClientMap) Length() int {
	return len(cmap.cmap)
}

type TimerMap struct {
	cmap map[string]*time.Timer
	sync.RWMutex
}

func NewTimerMap() *TimerMap {
	tmap := make(map[string]*time.Timer, 10)
	cmap := TimerMap{
		cmap: tmap,
	}
	return &cmap
}

func (tmap *TimerMap) Get(key string) (*time.Timer, bool) {
	tmap.RLock()
	defer tmap.RUnlock()
	timer, ok := tmap.cmap[key]
	return timer, ok
}

func (tmap *TimerMap) Set(key string, timer *time.Timer) {
	tmap.Lock()
	defer tmap.Unlock()
	tmap.cmap[key] = timer
}

func (tmap *TimerMap) Delete(key string) {
	tmap.RLock()
	defer tmap.RUnlock()
	delete(tmap.cmap, key)
}

func (tmap *TimerMap) Iter() <-chan *time.Timer {
	var ch = make(chan *time.Timer)
	go func() {
		for _, timer := range tmap.cmap {
			ch <- timer
		}
		close(ch)
	}()
	return ch
}

func (tmap *TimerMap) Length() int {
	return len(tmap.cmap)
}
