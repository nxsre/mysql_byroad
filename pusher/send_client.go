package main

import (
	"net/http"
	"sync"
	"time"
)

type SendClient struct {
	clients []*http.Client
	sync.RWMutex
}

func NewSendClient() *SendClient {
	sendClient := SendClient{
		clients: make([]*http.Client, 0, 10),
	}
	return &sendClient
}

func (this *SendClient) get(timeout time.Duration) *http.Client {
	for _, client := range this.clients {
		if client.Timeout == timeout {
			return client
		}
	}
	return nil
}

func (this *SendClient) add(timeout time.Duration) *http.Client {
	client := http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: Conf.MaxIdleConnsPerHost,
		},
		Timeout: timeout,
	}
	this.clients = append(this.clients, &client)
	return &client
}

func (this *SendClient) Get(timeout time.Duration) *http.Client {
	this.RLock()
	client := this.get(timeout)
	this.RUnlock()
	if client == nil {
		this.Lock()
		if this.get(timeout) == nil {
			client = this.add(timeout)
		}
		this.Unlock()
	}
	return client
}
