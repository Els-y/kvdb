package kv

import (
	"bytes"
	"encoding/gob"
	"go.uber.org/zap"
	"sync"
)

type KVActionType int

const (
	KVActionPut KVActionType = iota
	KVActionDel
)

type KVStore struct {
	mu      sync.RWMutex
	logger  *zap.Logger
	kvStore map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		kvStore: make(map[string]string),
	}
}

func (s *KVStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.kvStore[key]
	return v, ok
}

func (s *KVStore) Put(key, val string) {
	s.mu.Lock()
	s.kvStore[key] = val
	s.mu.Unlock()
}

func (s *KVStore) Del(key string) {
	s.mu.Lock()
	delete(s.kvStore, key)
	s.mu.Unlock()
}

type KVLog struct {
	Action KVActionType
	Key    string
	Val    string
}

func (s *KVStore) ProposePut(key, val string) string {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(KVLog{Action: KVActionPut, Key: key, Val: val}); err != nil {
		s.logger.Fatal("KVStore ProposePut error", zap.Error(err))
	}
	return buf.String()
}

func (s *KVStore) ProposeDel(key string) string {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(KVLog{Action: KVActionDel, Key: key}); err != nil {
		s.logger.Fatal("KVStore ProposeDel error", zap.Error(err))
	}
	return buf.String()
}

func (s *KVStore) DecodeLog(log string) KVLog {
	var kvlog KVLog
	dec := gob.NewDecoder(bytes.NewBufferString(log))
	if err := dec.Decode(&kvlog); err != nil {
		s.logger.Fatal("KVStore DecodeLog could not decode message", zap.Error(err))
	}
	return kvlog
}
