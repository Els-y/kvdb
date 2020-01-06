package server

import (
	"testing"
)

func TestNewServer(t *testing.T) {
	cfg := NewConfig(1, "data", "1=http://127.0.0.1:2200,2=http://127.0.0.1:3200,3=http://127.0.0.1:4200", 2201)
	server := NewServer(cfg)
	server.Start()
	t.Log("server running")
}

func TestNewServer_Single(t *testing.T) {
	cfg := NewConfig(1, "data", "1=http://127.0.0.1:2200", 2201)
	server := NewServer(cfg)
	server.Start()
	t.Log("server running")
}
