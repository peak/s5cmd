package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/peakgames/s5cmd"
	"log"
	"math"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

var (
	GitSummary, GitBranch string
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
	log.Printf("# Stats: %-6s %10d %4d ops/sec%s", name, counter, ops, extra)
}

func main() {
	const bytesInMb = float64(1024 * 1024)

	var (
		numWorkers         int
		cmdFile            string
		multipartChunkSize int
		retries            int
	)

	defaultPartSize := int(math.Ceil(float64(s3manager.DefaultUploadPartSize) / bytesInMb)) // Convert to MB

	flag.StringVar(&cmdFile, "f", "-", "Commands-file or - for stdin")
	flag.IntVar(&numWorkers, "numworkers", 256, fmt.Sprintf("Number of worker goroutines. Negative numbers mean multiples of runtime.NumCPU (currently %d)", runtime.NumCPU()))
	flag.IntVar(&multipartChunkSize, "cs", defaultPartSize, "Multipart chunk size in MB for uploads")
	flag.IntVar(&retries, "r", 10, "Retry S3 operations N times before failing")
	version := flag.Bool("version", false, "Prints current version")

	flag.Parse()

	if *version {
		fmt.Printf("s5cmd version %s (from branch %s)\n", GitSummary, GitBranch)
		os.Exit(0)
	}

	if cmdFile == "" || numWorkers == 0 || multipartChunkSize < 1 || retries < 0 {
		log.Fatal("-ERR Please specify all arguments.")
		os.Exit(1)
	}

	multipartChunkSizeBytes := int64(multipartChunkSize * int(bytesInMb))
	if multipartChunkSizeBytes < s3manager.MinUploadPartSize {
		log.Fatalf("-ERR Multipart chunk size should be bigger than %d", int(math.Ceil(float64(s3manager.MinUploadPartSize)/bytesInMb)))
	}

	if numWorkers < 0 {
		numWorkers = runtime.NumCPU() * -numWorkers
	}

	startTime := time.Now()

	log.Printf("# Using %d workers", numWorkers)

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
		log.Print("# Got signal, cleaning up...")
		cancelFunc()
	}()

	s := s5cmd.Stats{}

	s5cmd.NewWorkerPool(ctx,
		&s5cmd.WorkerPoolParams{
			NumWorkers:     numWorkers,
			ChunkSizeBytes: multipartChunkSizeBytes,
			Retries:        retries,
		}, &s).Run(cmdFile)

	elapsed := time.Since(startTime)

	log.Printf("# Exiting with code %d", exitCode)

	s3ops := s.Get(s5cmd.STATS_S3OP)
	fileops := s.Get(s5cmd.STATS_FILEOP)
	shellops := s.Get(s5cmd.STATS_SHELLOP)
	failops := s.Get(s5cmd.STATS_FAIL)
	printOps("S3", s3ops, elapsed, "")
	printOps("File", fileops, elapsed, "")
	printOps("Shell", shellops, elapsed, "")
	printOps("Failed", failops, elapsed, "")
	printOps("Total", s3ops+fileops+shellops+failops, elapsed, fmt.Sprintf(" %.2f seconds", elapsed.Seconds()))

	os.Exit(exitCode)
}
