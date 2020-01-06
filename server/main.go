package server

import "flag"

func Main() {
	id := flag.Uint64("id", 1, "node ID")
	data := flag.String("data", "data", "path to store wal")
	cluster := flag.String("cluster", "1=http://127.0.0.1:2200", "comma separated cluster peers")
	kvPort := flag.Int("port", 4396, "key-value server port")
	flag.Parse()

	cfg := NewConfig(*id, *data, *cluster, *kvPort)
	server := NewServer(cfg)
	server.Start()
}
