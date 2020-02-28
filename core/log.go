package core

import (
	"fmt"

	"github.com/peak/s5cmd/flags"
)

func verboseLog(format string, a ...interface{}) {
	if *flags.Verbose {
		fmt.Printf("VERBOSE: "+format+"\n", a...)
	}
}
