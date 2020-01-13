package cmd

import (
	"fmt"
	pb "github.com/Els-y/kvdb/rpc"
	"github.com/spf13/cobra"
)

func NewDelCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "del <key>",
		Short: "Removes the specified key",
		Run:   delCommandFunc,
	}
}

func delCommandFunc(cmd *cobra.Command, args []string) {
	key := getDelOp(args)

	ctx, cancel := commandCtx(cmd)
	resp, err := mustClientFromCmd(cmd).Del(ctx, &pb.DelRequest{Key: []byte(key)})
	defer cancel()
	if err != nil {
		ExitWithError(ExitError, err)
	}
	if resp.Ok {
		fmt.Println("ok")
	}
}

func getDelOp(args []string) string {
	if len(args) != 1 {
		ExitWithError(ExitBadArgs, fmt.Errorf("del command needs 1 arguments"))
	}

	key := args[0]
	return key
}
