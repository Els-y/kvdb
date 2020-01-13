package cmd

import (
	"fmt"
	pb "github.com/Els-y/kvdb/rpc"
	"github.com/spf13/cobra"
)

func NewGetCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "get <key>",
		Short: "Gets the key",
		Run:   getCommandFunc,
	}
}

func getCommandFunc(cmd *cobra.Command, args []string) {
	key := getGetOp(args)

	ctx, cancel := commandCtx(cmd)
	resp, err := mustClientFromCmd(cmd).Get(ctx, &pb.GetRequest{Key: []byte(key)})
	defer cancel()
	if err != nil {
		ExitWithError(ExitError, err)
	}
	if resp.Ok {
		fmt.Println(key)
		fmt.Println(string(resp.Val))
	}
}

func getGetOp(args []string) string {
	if len(args) != 1 {
		ExitWithError(ExitBadArgs, fmt.Errorf("get command needs 1 arguments"))
	}

	key := args[0]
	return key
}
