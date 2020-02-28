//go:generate go run version/cmd/generate.go

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/gops/agent"
	"github.com/peak/s5cmd/complete"
	"github.com/peak/s5cmd/core"
	"github.com/peak/s5cmd/flags"
	"github.com/peak/s5cmd/stats"
	"github.com/peak/s5cmd/version"
)

func printOps(name string, counter uint64, elapsed time.Duration, extra string) {
	if counter == 0 {
		return
	}

	secs := elapsed.Seconds()
	if secs == 0 {
		secs = 1
	}

	ops := uint64(math.Floor((float64(counter) / secs) + 0.5))
	log.Printf("# Stats: %-7s %10d %4d ops/sec%s", name, counter, ops, extra)
}

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
		log.Fatal("-ERR " + err.Error())
	} else if done {
		os.Exit(0)
	}

	if *flags.ShowVersion {
		fmt.Println(version.GetHumanVersion())
		os.Exit(0)
	}

	if *flags.EnableGops || os.Getenv("S5CMD_GOPS") != "" {
		if err := agent.Listen(&agent.Options{NoShutdownCleanup: true}); err != nil {
			log.Fatal("-ERR", err)
		}
	}

	// validation must be done after the completion
	if err := flags.Validate(); err != nil {
		log.Print(err)
		os.Exit(2)
	}

	cmd := strings.Join(flag.Args(), " ")

	var cmdMode bool
	if cmd != "" {
		cmdMode = true
	}

	parentCtx, cancelFunc := context.WithCancel(context.Background())

	ctx := context.WithValue(
		parentCtx,
		core.CancelFuncKey,
		cancelFunc,
	)

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		log.Print("# Got signal, cleaning up...")
		cancelFunc()
	}()

	wp := core.NewWorkerManager(ctx)
	if cmdMode {
		wp.RunCmd(ctx, cmd)
	} else {
		wp.Run(ctx, *flags.CommandFile)
	}

	failops := stats.Get(stats.Fail)

	exitCode := 0
	if failops > 0 {
		// TODO(ig): should return 1 for errors.
		exitCode = 127
	}

	if !cmdMode {
		log.Printf("# Exiting with code %d", exitCode)
	}

	if !cmdMode || *flags.PrintStats {
		elapsed := stats.Elapsed()

		s3ops := stats.Get(stats.S3Op)
		fileops := stats.Get(stats.FileOp)

		printOps("S3", s3ops, elapsed, "")
		printOps("File", fileops, elapsed, "")
		printOps("Failed", failops, elapsed, "")
		printOps("Total", s3ops+fileops+failops, elapsed, fmt.Sprintf(" %v", elapsed))
	}

	os.Exit(exitCode)
}
