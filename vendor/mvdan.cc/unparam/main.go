// Copyright (c) 2017, Daniel Martí <mvdan@mvdan.cc>
// See LICENSE for licensing information

// unparam reports unused function parameters and results in your code.
package main

import (
	"flag"
	"fmt"
	"os"

	"mvdan.cc/unparam/check"
)

var (
	flagSet = flag.NewFlagSet("unparam", flag.ExitOnError)

	tests    = flagSet.Bool("tests", false, "load tests too")
	exported = flagSet.Bool("exported", false, "inspect exported functions")
	debug    = flagSet.Bool("debug", false, "debug prints")
)

func main() {
	flagSet.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: unparam [flags] [package ...]")
		flagSet.PrintDefaults()
	}
	flagSet.Parse(os.Args[1:])
	warns, err := check.UnusedParams(*tests, *exported, *debug, flagSet.Args()...)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, warn := range warns {
		fmt.Println(warn)
	}
	if len(warns) > 0 {
		os.Exit(1)
	}
}
