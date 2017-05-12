package core

import "fmt"

// Verbose is an ugly global variable for verbose output, mainly for debugging
var Verbose bool

func verboseLog(format string, a ...interface{}) {
	if Verbose {
		fmt.Printf("VERBOSE: "+format+"\n", a...)
	}
}
