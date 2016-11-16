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
	cmdFile := flag.String("f", "-", "Commands-file or - for stdin")
	numWorkers := flag.Int("numworkers", runtime.NumCPU(), "Number of worker goroutines.")
	version := flag.Bool("version", false, "Prints current version")

	flag.Parse()

	if *version {
		fmt.Printf("s5cmd version %s (from branch %s)\n", GitSummary, GitBranch)
		os.Exit(0)
	}

	if *cmdFile == "" {
		log.Fatal("Please specify all arguments.")
	}

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
		log.Println("Got signal, cleaning up...")
		cancelFunc()
	}()

	s5cmd.NewWorkerPool(ctx,
		&s5cmd.WorkerPoolParams{
			NumWorkers: *numWorkers,
		}).Run(*cmdFile)

	log.Printf("Exiting with code %d", exitCode)
	os.Exit(exitCode)
}
