package main

import (
	"testing"
	"time"
)

func TestClientRace(t *testing.T) {
	clients := NewSendClient()
	go func() {
		for {
			clients.Get(time.Second)
		}
	}()
	for {
		clients.Get(time.Second)
	}
}
