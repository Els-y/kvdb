package main

import (
	"context"
	"fmt"
	pb "github.com/Els-y/kvdb/rpc"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"strconv"
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

func (b *Bencher) run() {
	numReqPerClient := b.NumRequest / b.NumClient
	log.Printf("numRequest: %d, numClient: %d, numReqPerClient: %d\n", b.NumRequest, b.NumClient, numReqPerClient)

	startTime := time.Now()
	b.wg.Add(b.NumClient)
	for i := 0; i < b.NumClient; i++ {
		go b.startClient(i, numReqPerClient)
	}
	b.wg.Wait()
	endTime := time.Now()
	duration := endTime.Sub(startTime)
	log.Printf("duration: %v, qps: %f\n", duration, float64(b.NumRequest)/duration.Seconds())
}

func (b *Bencher) startClient(id, numRequest int) {
	client := b.initClient()
	keyStart := id * numRequest
	keyEnd := keyStart + numRequest
	for i := keyStart; i < keyEnd; i++ {
		key := keyWithZeroPad(i, b.KeySize)
		err := b.execOperation(key, client)
		if err != nil {
			log.Fatal("client exec operation fail", err)
		}
	}
	b.wg.Done()
}

func (b *Bencher) initClient() pb.KVClient {
	endpoint := b.randomPickEndpoint()
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

func (b *Bencher) randomPickEndpoint() string {
	if len(b.Endpoints) == 1 {
		return b.Endpoints[0]
	}
	index := rand.Intn(len(b.Endpoints))
	return b.Endpoints[index]
}

func (b *Bencher) execOperation(key []byte, client pb.KVClient) error {
	var err error
	if b.Op == Write {
		err = b.execWriteOperation(key, client)
	} else {
		err = b.execReadOperation(key, client)
	}
	return err
}

func (b *Bencher) execWriteOperation(key []byte, client pb.KVClient) error {
	value := getRandomBytes(b.ValueSize)

	ctx := context.TODO()
	_, err := client.Put(ctx, &pb.PutRequest{Key: key, Val: value})
	return err
}

func (b *Bencher) execReadOperation(key []byte, client pb.KVClient) error {
	ctx := context.TODO()
	_, err := client.Get(ctx, &pb.GetRequest{Key: key})
	return err
}

func keyWithZeroPad(val, length int) []byte {
	key := fmt.Sprintf("%0"+strconv.Itoa(length)+"d", val)
	return []byte(key)
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
		Op:                    Write,
		NumClient:             10,
		NumRequest:            1000,
		KeySize:               8,
		ValueSize:             256,
		DefaultDialTimeout:    2 * time.Second,
		DefaultCommandTimeOut: 5 * time.Second,
		Endpoints:             []string{"127.0.0.1:12380", "127.0.0.1:22380", "127.0.0.1:32380"},
	}
	bencher.run()
}
