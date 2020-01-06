package kv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestKvstore(t *testing.T) {
	kvStore := NewKVStore()
	kvStore.Put("abc", "def")
	val, ok := kvStore.Get("abc")
	assert.True(t, ok)
	assert.Equal(t, "def", val)

	kvStore.Del("abc")
	_, ok = kvStore.Get("abc")
	assert.False(t, ok)
}
