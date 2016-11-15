package main

import (
	"context"
	"flag"
	"github.com/peakgames/s5cmd"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

func main() {
	cmdFile := flag.String("f", "-", "Commands-file or - for stdin")
	numWorkers := flag.Int("numworkers", runtime.NumCPU(), "Number of worker goroutines.")

	flag.Parse()

	if *cmdFile == "" {
		log.Fatal("Please specify all arguments.")
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

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

	log.Println("Exiting")
}
