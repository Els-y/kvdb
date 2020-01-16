package main

import (
	"context"
	pb "github.com/Els-y/kvdb/rpc"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

type OpType int

const (
	Write OpType = iota
	Read
)

type Bencher struct {
	Op         OpType
	NumClient  int
	NumRequest int
	KeySize    int
	ValueSize  int

	DefaultDialTimeout    time.Duration
	DefaultCommandTimeOut time.Duration
	Endpoints             []string
	wg                    sync.WaitGroup
}

func (b *Bencher) Start() {
	numReqPerClient := b.NumRequest / b.NumClient
	log.Printf("numRequest: %d, numClient: %d, numReqPerClient: %d\n", b.NumRequest, b.NumClient, numReqPerClient)

	b.wg.Add(b.NumClient)
	startTime := time.Now()
	for i := 0; i < b.NumClient; i++ {
		go b.startClient(numReqPerClient)
	}
	b.wg.Wait()
	endTime := time.Now()
	duration := endTime.Sub(startTime)
	log.Printf("duration: %v, qps: %f\n", duration, float64(b.NumRequest)/duration.Seconds())
}

func (b *Bencher) startClient(numRequest int) {
	client := b.initClient()
	for i := 0; i < numRequest; i++ {
		err := b.execOperation(client)
		if err != nil {
			log.Fatal("client exec operation fail", err)
		}
	}
	b.wg.Done()
}

func (b *Bencher) initClient() pb.KVClient {
	endpoint := b.pickEndpoint()
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, b.DefaultDialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, endpoint, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatal("could not connect to server", err)
	}
	client := pb.NewKVClient(conn)
	return client
}

func (b *Bencher) pickEndpoint() string {
	if len(b.Endpoints) == 1 {
		return b.Endpoints[0]
	}
	index := rand.Intn(len(b.Endpoints) - 1)
	return b.Endpoints[index]
}

func (b *Bencher) execOperation(client pb.KVClient) error {
	var err error
	if b.Op == Write {
		err = b.execWriteOperation(client)
	} else {
		err = b.execReadOperation(client)
	}
	return err
}

func (b *Bencher) execWriteOperation(client pb.KVClient) error {
	key := getRandomBytes(b.KeySize)
	value := getRandomBytes(b.ValueSize)

	ctx := context.TODO()
	_, err := client.Put(ctx, &pb.PutRequest{Key: key, Val: value})
	return err
}

func (b *Bencher) execReadOperation(client pb.KVClient) error {
	key := getRandomBytes(b.KeySize)

	ctx := context.TODO()
	_, err := client.Get(ctx, &pb.GetRequest{Key: key})
	return err
}

func getRandomBytes(l int) []byte {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	bytes := []byte(str)
	var result []byte
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < l; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return result
}

func main() {
	bencher := &Bencher{
		Op:                    Read,
		NumClient:             10,
		NumRequest:            1000,
		KeySize:               8,
		ValueSize:             256,
		DefaultDialTimeout:    2 * time.Second,
		DefaultCommandTimeOut: 5 * time.Second,
		Endpoints:             []string{"127.0.0.1:22380"},
		wg:                    sync.WaitGroup{},
	}
	bencher.Start()
}
