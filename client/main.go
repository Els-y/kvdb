package main

import (
	"context"
	pb "github.com/Els-y/kvdb/rpc"
	"google.golang.org/grpc"
	"log"
	"time"
)

var (
	address = "127.0.0.1:32380"
	//address = "127.0.0.1:2201"
)

func main() {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, address, grpc.WithInsecure(), grpc.WithBlock())
	log.Print("conn start")
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKVClient(conn)

	resp, err := c.Put(context.TODO(), &pb.PutRequest{Key: []byte("abc"), Val: []byte("def")})
	if err != nil {
		log.Panic("err=", err)
	}
	log.Print("resp:", resp.Ok)
}
