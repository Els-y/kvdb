package cmd

import (
	"context"
	pb "github.com/Els-y/kvdb/rpc"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"time"
)

type GlobalFlags struct {
	Endpoint       string
	DialTimeout    time.Duration
	CommandTimeOut time.Duration
}

func commandCtx(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	timeOut, err := cmd.Flags().GetDuration("command-timeout")
	if err != nil {
		ExitWithError(ExitError, err)
	}
	return context.WithTimeout(context.Background(), timeOut)
}

func dialTimeoutFromCmd(cmd *cobra.Command) time.Duration {
	dialTimeout, err := cmd.Flags().GetDuration("dial-timeout")
	if err != nil {
		ExitWithError(ExitError, err)
	}
	return dialTimeout
}

func endpointFromCmd(cmd *cobra.Command) string {
	endpoint, err := cmd.Flags().GetString("endpoint")
	if err != nil {
		ExitWithError(ExitError, err)
	}
	return endpoint
}

func mustClientFromCmd(cmd *cobra.Command) pb.KVClient {
	dialTimeout := dialTimeoutFromCmd(cmd)
	endpoint := endpointFromCmd(cmd)

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, endpoint, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		ExitWithError(ExitError, err)
	}
	client := pb.NewKVClient(conn)
	return client
}
