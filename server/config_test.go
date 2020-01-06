package server

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseCluster(t *testing.T) {
	ids, urls, port := parseCluster(
		"1=http://127.0.0.1:1234,2=http://127.0.0.1:2345,3=http://127.0.0.1:3456", 1)
	assert.Equal(t, 1234, port)
	for index := range ids {
		id := ids[index]
		url := urls[index]
		fmt.Println(id, url)
	}

	ids, urls, port = parseCluster(
		"1=http://127.0.0.1:1234,2=http://127.0.0.1:2345,3=http://127.0.0.1:3456", 2)
	assert.Equal(t, 2345, port)
	for index := range ids {
		id := ids[index]
		url := urls[index]
		fmt.Println(id, url)
	}

	ids, urls, port = parseCluster(
		"1=http://127.0.0.1:1234,2=http://127.0.0.1:2345,3=http://127.0.0.1:3456", 3)
	assert.Equal(t, 3456, port)
	for index := range ids {
		id := ids[index]
		url := urls[index]
		fmt.Println(id, url)
	}
}
