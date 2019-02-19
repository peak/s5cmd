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

func main() {
	const bytesInMb = float64(1024 * 1024)
	const minNumWorkers = 2
	const defaultNumWorkers = 256

	var (
		numWorkers    int
		cmdFile       string
		endpointURL   string
		ulPartSize    int
		ulConcurrency int
		dlPartSize    int
		dlConcurrency int
		retries       int
		noVerify      bool
	)

	flag.StringVar(&cmdFile, "f", "", "Commands-file or - for stdin")
	flag.StringVar(&endpointURL, "endpoint-url", "", "Override default URL with the given one")
	flag.IntVar(&numWorkers, "numworkers", defaultNumWorkers, fmt.Sprintf("Number of worker goroutines. Negative numbers mean multiples of the CPU core count."))
	flag.IntVar(&dlConcurrency, "dw", s3manager.DefaultDownloadConcurrency, "Download concurrency (single file)")
	flag.IntVar(&dlPartSize, "ds", 50, "Multipart chunk size in MB for downloads")
	flag.IntVar(&ulConcurrency, "uw", s3manager.DefaultUploadConcurrency, "Upload concurrency (single file)")
	flag.IntVar(&ulPartSize, "us", 50, "Multipart chunk size in MB for uploads")
	flag.IntVar(&retries, "r", 10, "Retry S3 operations N times before failing")
	printStats := flag.Bool("stats", false, "Always print stats")
	showVersion := flag.Bool("version", false, "Prints current version")
	gops := flag.Bool("gops", false, "Initialize gops agent")
	verbose := flag.Bool("vv", false, "Verbose output")
	flag.BoolVar(&noVerify, "no-verify", false, "Don't verify SSL certificates")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%v\n\n", core.UsageLine())

		fmt.Fprint(os.Stderr, "Options:\n")
		flag.PrintDefaults()

		cl := core.CommandList()
		fmt.Fprint(os.Stderr, "\nCommands:")
		fmt.Fprintf(os.Stderr, "\n    %v\n", strings.Join(cl, ", "))
		fmt.Fprintf(os.Stderr, "\nTo get help on a specific command, run \"%v <command> -h\"\n", os.Args[0])
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
		flag.Usage()
		os.Exit(2)
	}

	cmd := strings.Join(flag.Args(), " ")
	if cmd != "" && cmdFile != "" {
		log.Fatal("-ERR Only specify -f or command, not both")
	}
	if (cmd == "" && cmdFile == "") || numWorkers == 0 || ulPartSize < 1 || retries < 0 {
		log.Fatal("-ERR Please specify all arguments.")
	}

	ulPartSizeBytes := int64(ulPartSize * int(bytesInMb))
	if ulPartSizeBytes < s3manager.MinUploadPartSize {
		log.Fatalf("-ERR Multipart chunk size should be greater than %d", int(math.Ceil(float64(s3manager.MinUploadPartSize)/bytesInMb)))
	}
	dlPartSizeBytes := int64(dlPartSize * int(bytesInMb))
	if dlPartSizeBytes < int64(5*bytesInMb) {
		log.Fatalf("-ERR Download part size should be greater than 5")
	}
	if dlConcurrency < 1 || ulConcurrency < 1 {
		log.Fatalf("-ERR Download/Upload concurrency should be greater than 1")
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

	endpointURL = strings.TrimSpace(endpointURL)

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
			NumWorkers:             numWorkers,
			UploadChunkSizeBytes:   ulPartSizeBytes,
			UploadConcurrency:      ulConcurrency,
			DownloadChunkSizeBytes: dlPartSizeBytes,
			DownloadConcurrency:    dlConcurrency,
			Retries:                retries,
			EndpointURL:            endpointURL,
			NoVerifySSL:            noVerify,
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
