package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/peakgames/s5cmd"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

var (
	GitSummary, GitBranch string
)

func main() {
	var (
		numWorkers int
		cmdFile    string
	)

	flag.StringVar(&cmdFile, "f", "-", "Commands-file or - for stdin")
	flag.IntVar(&numWorkers, "numworkers", 256, fmt.Sprintf("Number of worker goroutines. Negative numbers mean multiples of runtime.NumCPU (currently %d)", runtime.NumCPU()))
	version := flag.Bool("version", false, "Prints current version")

	flag.Parse()

	if *version {
		fmt.Printf("s5cmd version %s (from branch %s)\n", GitSummary, GitBranch)
		os.Exit(0)
	}

	if cmdFile == "" || numWorkers == 0 {
		log.Fatal("Please specify all arguments.")
		os.Exit(1)
	}

	if numWorkers < 0 {
		numWorkers = runtime.NumCPU() * -numWorkers
	}
	log.Printf("Using %d workers", numWorkers)

	parentCtx, cancelFunc := context.WithCancel(context.Background())

	var exitCode int
	exitFunc := func(code int) {
		//log.Printf("Called exitFunc with code %d", code)
		exitCode = code
		cancelFunc()
	}

	ctx := context.WithValue(context.WithValue(parentCtx, "exitFunc", exitFunc), "cancelFunc", cancelFunc)

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		log.Print("Got signal, cleaning up...")
		cancelFunc()
	}()

	s5cmd.NewWorkerPool(ctx,
		&s5cmd.WorkerPoolParams{
			NumWorkers: numWorkers,
		}).Run(cmdFile)

	log.Printf("Exiting with code %d", exitCode)
	os.Exit(exitCode)
}
