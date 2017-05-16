package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/gops/agent"

	"github.com/peakgames/s5cmd/complete"
	"github.com/peakgames/s5cmd/core"
	"github.com/peakgames/s5cmd/opt"
	"github.com/peakgames/s5cmd/stats"
	"github.com/peakgames/s5cmd/version"
)

//go:generate go run version/cmd/generate.go
var (
	GitSummary = version.GitSummary
	GitBranch  = version.GitBranch
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

func printUsageLine() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTION]... [COMMAND [PARAMS...]]\n\n", os.Args[0])
}

func main() {
	const bytesInMb = float64(1024 * 1024)
	const minNumWorkers = 2
	const defaultNumWorkers = 256

	var (
		numWorkers         int
		cmdFile            string
		multipartChunkSize int
		retries            int
	)

	defaultPartSize := int(math.Ceil(float64(s3manager.DefaultUploadPartSize) / bytesInMb)) // Convert to MB

	flag.StringVar(&cmdFile, "f", "", "Commands-file or - for stdin")
	flag.IntVar(&numWorkers, "numworkers", defaultNumWorkers, fmt.Sprintf("Number of worker goroutines. Negative numbers mean multiples of the CPU core count."))
	flag.IntVar(&multipartChunkSize, "cs", defaultPartSize, "Multipart chunk size in MB for uploads")
	flag.IntVar(&retries, "r", 10, "Retry S3 operations N times before failing")
	printStats := flag.Bool("stats", false, "Always print stats")
	showVersion := flag.Bool("version", false, "Prints current version")
	gops := flag.Bool("gops", false, "Initialize gops agent")
	verbose := flag.Bool("vv", false, "Verbose output")

	flag.Usage = func() {
		printUsageLine()
		flag.PrintDefaults()
		fmt.Fprint(os.Stderr, "\nTo list available commands, run without arguments.\n")
	}

	//flag.Parse()
	if done, err := complete.ParseFlagsAndRun(); err != nil {
		log.Fatal("-ERR " + err.Error())
	} else if done {
		os.Exit(0)
	}

	if *gops || os.Getenv("S5CMD_GOPS") != "" {
		if err := agent.Listen(&agent.Options{NoShutdownCleanup: true}); err != nil {
			log.Fatal("-ERR", err)
		}
	}

	if *showVersion {
		fmt.Printf("s5cmd version %s", GitSummary)
		if GitBranch != "" {
			fmt.Printf(" (from branch %s)", GitBranch)
		}
		fmt.Print("\n")
		os.Exit(0)
	}

	if flag.Arg(0) == "" && cmdFile == "" {
		printUsageLine()

		fmt.Fprint(os.Stderr, "Command options:\n")
		fmt.Fprintf(os.Stderr, opt.GetOptionList())
		fmt.Fprint(os.Stderr, "\n")

		fmt.Fprint(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, core.GetCommandList())
		fmt.Fprint(os.Stderr, "\nTo list available options, run with the -h option.\n")
		os.Exit(2)
	}

	cmd := strings.Join(flag.Args(), " ")
	if cmd != "" && cmdFile != "" {
		log.Fatal("-ERR Only specify -f or command, not both")
	}
	if (cmd == "" && cmdFile == "") || numWorkers == 0 || multipartChunkSize < 1 || retries < 0 {
		log.Fatal("-ERR Please specify all arguments.")
	}

	multipartChunkSizeBytes := int64(multipartChunkSize * int(bytesInMb))
	if multipartChunkSizeBytes < s3manager.MinUploadPartSize {
		log.Fatalf("-ERR Multipart chunk size should be bigger than %d", int(math.Ceil(float64(s3manager.MinUploadPartSize)/bytesInMb)))
	}

	var cmdMode bool
	if cmd != "" {
		cmdMode = true
	}
	if numWorkers < 0 {
		numWorkers = runtime.NumCPU() * -numWorkers
	}
	if numWorkers < minNumWorkers {
		numWorkers = minNumWorkers
	}

	startTime := time.Now()

	if !cmdMode {
		log.Printf("# Using %d workers", numWorkers)
	}

	parentCtx, cancelFunc := context.WithCancel(context.Background())

	exitCode := -1
	exitFunc := func(code int) {
		//log.Printf("Called exitFunc with code %d", code)
		exitCode = code
		cancelFunc()
	}

	ctx := context.WithValue(context.WithValue(parentCtx, core.ExitFuncKey, exitFunc), core.CancelFuncKey, cancelFunc)

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		log.Print("# Got signal, cleaning up...")
		cancelFunc()
	}()

	s := stats.Stats{}

	core.Verbose = *verbose

	wp := core.NewWorkerPool(ctx,
		&core.WorkerPoolParams{
			NumWorkers:     numWorkers,
			ChunkSizeBytes: multipartChunkSizeBytes,
			Retries:        retries,
		}, &s)
	if cmdMode {
		wp.RunCmd(cmd)
	} else {
		wp.Run(cmdFile)
	}

	elapsed := time.Since(startTime)

	failops := s.Get(stats.Fail)

	// if exitCode is -1 (default) and if we have at least one absolute-fail, exit with code 127
	if exitCode == -1 {
		if failops > 0 {
			exitCode = 127
		} else {
			exitCode = 0
		}
	}

	if !cmdMode {
		log.Printf("# Exiting with code %d", exitCode)
	}

	if !cmdMode || *printStats {
		s3ops := s.Get(stats.S3Op)
		fileops := s.Get(stats.FileOp)
		shellops := s.Get(stats.ShellOp)
		retryops := s.Get(stats.RetryOp)
		printOps("S3", s3ops, elapsed, "")
		printOps("File", fileops, elapsed, "")
		printOps("Shell", shellops, elapsed, "")
		printOps("Retried", retryops, elapsed, "")
		printOps("Failed", failops, elapsed, "")

		printOps("Total", s3ops+fileops+shellops+failops, elapsed, fmt.Sprintf(" %v", elapsed))
	}

	os.Exit(exitCode)
}
