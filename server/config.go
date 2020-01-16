package server

import (
	log "github.com/Els-y/kvdb/pkg/logutil"
	"go.uber.org/zap"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ID       uint64
	PeersID  []uint64
	PeersURL []url.URL

	PeerServerPort   int
	ClientServerPort int

	ElectionTick  int
	HeartbeatTick int
	IntervalTick  time.Duration

	WalDir string

	Logger *zap.Logger
}

func NewConfig(id uint64, dataDir, cluster string, port int) *Config {
	peersID, peersURL, peerServerPort := parseCluster(cluster, id)
	return &Config{
		ID:               id,
		PeersID:          peersID,
		PeersURL:         peersURL,
		PeerServerPort:   peerServerPort,
		ClientServerPort: port,
		ElectionTick:     10,
		HeartbeatTick:    1,
		IntervalTick:     150 * time.Millisecond,
		WalDir:           dataDir,
		Logger:           zap.NewExample(),
	}
}

func parseCluster(cluster string, localId uint64) ([]uint64, []url.URL, int) {
	var (
		ids  []uint64
		urls []url.URL
	)
	peerServerPort := -1

	for _, member := range strings.Split(cluster, ",") {
		parts := strings.Split(member, "=")
		strId := parts[0]
		strUrl := parts[1]

		u, err := url.Parse(strUrl)
		if err != nil {
			log.Panic("invalid cluster",
				zap.String("cluster", cluster))
		}

		id, err := strconv.ParseUint(strId, 10, 64)
		if err != nil {
			log.Panic("invalid cluster",
				zap.String("cluster", cluster))
		}

		if id == localId {
			parts := strings.Split(u.Host, ":")
			peerServerPort, _ = strconv.Atoi(parts[1])
		} else {
			urls = append(urls, *u)
			ids = append(ids, id)
		}
	}
	return ids, urls, peerServerPort
}
