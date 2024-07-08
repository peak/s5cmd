// Copyright (c) 2017, Daniel Martí <mvdan@mvdan.cc>
// See LICENSE for licensing information

package main

import (
	"flag"
	"fmt"
	"os"

	"mvdan.cc/unparam/check"
)

var (
	flagSet = flag.NewFlagSet("unparam", flag.ContinueOnError)

	tests    = flagSet.Bool("tests", false, "load tests too")
	exported = flagSet.Bool("exported", false, "inspect exported functions")
	debug    = flagSet.Bool("debug", false, "debug prints")
)

func main() {
	os.Exit(main1())
}

func main1() int {
	flagSet.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: unparam [flags] [package ...]")
		flagSet.PrintDefaults()
	}
	if err := flagSet.Parse(os.Args[1:]); err != nil {
		if err != flag.ErrHelp {
			fmt.Fprintln(os.Stderr, err)
		}
		return 1
	}
	warns, err := check.UnusedParams(*tests, *exported, *debug, flagSet.Args()...)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	for _, warn := range warns {
		fmt.Println(warn)
	}
	if len(warns) > 0 {
		return 1
	}
	return 0
}
