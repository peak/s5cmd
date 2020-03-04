//go:generate go run version/cmd/generate.go

package main

import (
	"context"
	"flag"
	"fmt"
	stdlog "log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/google/gops/agent"
	"github.com/peak/s5cmd/complete"
	"github.com/peak/s5cmd/core"
	"github.com/peak/s5cmd/flags"
	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/stats"
	"github.com/peak/s5cmd/version"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%v\n\n", core.UsageLine())

		fmt.Fprint(os.Stderr, "Options:\n")
		flag.PrintDefaults()

		cl := core.CommandList()
		fmt.Fprint(os.Stderr, "\nCommands:")
		fmt.Fprintf(os.Stderr, "\n    %v\n", strings.Join(cl, ", "))
		fmt.Fprintf(os.Stderr, "\nTo get help on a specific command, run \"%v <command> -h\"\n", os.Args[0])
	}

	flags.Parse()

	if done, err := complete.ParseFlagsAndRun(); err != nil {
		stdlog.Fatal("-ERR " + err.Error())
	} else if done {
		os.Exit(0)
	}

	if *flags.ShowVersion {
		fmt.Println(version.GetHumanVersion())
		os.Exit(0)
	}

	if *flags.EnableGops || os.Getenv("S5CMD_GOPS") != "" {
		if err := agent.Listen(&agent.Options{NoShutdownCleanup: true}); err != nil {
			stdlog.Fatal("-ERR", err)
		}
	}

	// validation must be done after the completion
	if err := flags.Validate(); err != nil {
		stdlog.Print(err)
		os.Exit(2)
	}

	cmd := strings.Join(flag.Args(), " ")

	var cmdMode bool
	if cmd != "" {
		cmdMode = true
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		stdlog.Print("# Got signal, cleaning up...")
		cancel()
	}()

	log.Init()

	wp := core.NewWorkerManager(cancel)
	if cmdMode {
		wp.RunCmd(ctx, cmd)
	} else {
		wp.Run(ctx, *flags.CommandFile)
	}

	exitCode := 0
	if stats.HasFailed() {
		// TODO(ig): should return 1 for errors.
		exitCode = 127
	}

	os.Exit(exitCode)
}
