package main

import (
	"github.com/Els-y/kvdb/kvdbctl/cmd"
	"github.com/spf13/cobra"
	"time"
)

var (
	cliName        = "kvdbctl"
	cliDescription = "A simple command line client for kvdb."

	defaultDialTimeout    = 2 * time.Second
	defaultCommandTimeOut = 5 * time.Second
)

var (
	globalFlags = cmd.GlobalFlags{}
)

var (
	rootCmd = &cobra.Command{
		Use:        cliName,
		Short:      cliDescription,
		SuggestFor: []string{"kvdbctl"},
	}
)

func init() {
	rootCmd.PersistentFlags().StringVar(&globalFlags.Endpoint, "endpoint", "127.0.0.1:4396", "gRPC endpoints")
	rootCmd.PersistentFlags().DurationVar(&globalFlags.DialTimeout, "dial-timeout", defaultDialTimeout, "dial timeout for client connections")
	rootCmd.PersistentFlags().DurationVar(&globalFlags.CommandTimeOut, "command-timeout", defaultCommandTimeOut, "timeout for short running command (excluding dial timeout)")
	rootCmd.AddCommand(cmd.NewPutCommand())
	rootCmd.AddCommand(cmd.NewGetCommand())
	rootCmd.AddCommand(cmd.NewDelCommand())
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		cmd.ExitWithError(cmd.ExitError, err)
	}
}
