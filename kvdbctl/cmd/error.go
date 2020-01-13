package cmd

import (
	"fmt"
	"os"
)

const (
	// http://tldp.org/LDP/abs/html/exitcodes.html
	ExitSuccess = iota
	ExitError
	ExitBadArgs = 128
)

func ExitWithError(code int, err error) {
	fmt.Fprintln(os.Stderr, "Error:", err)
	os.Exit(code)
}
