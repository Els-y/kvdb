package cmd

import (
	"fmt"
	pb "github.com/Els-y/kvdb/rpc"
	"github.com/spf13/cobra"
)

func NewPutCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "put <key> <value>",
		Short: "Puts the given key into the store",
		Run:   putCommandFunc,
	}
}

func putCommandFunc(cmd *cobra.Command, args []string) {
	key, value := getPutOp(args)

	ctx, cancel := commandCtx(cmd)
	resp, err := mustClientFromCmd(cmd).Put(ctx, &pb.PutRequest{Key: []byte(key), Val: []byte(value)})
	defer cancel()
	if err != nil {
		ExitWithError(ExitError, err)
	}
	if resp.Ok {
		fmt.Println("ok")
	}
}

func getPutOp(args []string) (string, string) {
	if len(args) != 2 {
		ExitWithError(ExitBadArgs, fmt.Errorf("put command needs 2 arguments"))
	}

	key := args[0]
	value := args[1]
	return key, value
}
