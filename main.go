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

	"github.com/google/gops/agent"
	"github.com/peak/s5cmd/complete"
	"github.com/peak/s5cmd/core"
	"github.com/peak/s5cmd/stats"
	"github.com/peak/s5cmd/version"
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
	const (
		bytesInMb                  = float64(1024 * 1024)
		minNumWorkers              = 2
		defaultWorkerCount         = 256
		defaultUploadConcurrency   = 5
		defaultDownloadConcurrency = 5
		minUploadPartSize          = 5 * bytesInMb
	)

	var (
		flagCommandFile         = flag.String("f", "", "Commands-file or - for stdin")
		flagEndpointURL         = flag.String("endpoint-url", "", "Override default URL with the given one")
		flagWorkerCount         = flag.Int("numworkers", defaultWorkerCount, fmt.Sprintf("Number of worker goroutines. Negative numbers mean multiples of the CPU core count."))
		flagDownloadConcurrency = flag.Int("dw", defaultDownloadConcurrency, "Download concurrency for each file")
		flagDownloadPartSize    = flag.Int("ds", 50, "Multipart chunk size in MB for downloads")
		flagUploadConcurrency   = flag.Int("uw", defaultUploadConcurrency, "Upload concurrency for each file")
		flagUploadPartSize      = flag.Int("us", 50, "Multipart chunk size in MB for uploads")
		flagRetryCount          = flag.Int("r", 10, "Retry S3 operations N times before failing")
		flagPrintStats          = flag.Bool("stats", false, "Always print stats")
		flagShowVersion         = flag.Bool("version", false, "Prints current version")
		flagEnableGops          = flag.Bool("gops", false, "Initialize gops agent")
		flagVerbose             = flag.Bool("vv", false, "Verbose output")
		flagNoVerifySSL         = flag.Bool("no-verify-ssl", false, "Don't verify SSL certificates")
	)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%v\n\n", core.UsageLine())

		fmt.Fprint(os.Stderr, "Options:\n")
		flag.PrintDefaults()

		cl := core.CommandList()
		fmt.Fprint(os.Stderr, "\nCommands:")
		fmt.Fprintf(os.Stderr, "\n    %v\n", strings.Join(cl, ", "))
		fmt.Fprintf(os.Stderr, "\nTo get help on a specific command, run \"%v <command> -h\"\n", os.Args[0])
	}

	if done, err := complete.ParseFlagsAndRun(); err != nil {
		log.Fatal("-ERR " + err.Error())
	} else if done {
		os.Exit(0)
	}

	if *flagEnableGops || os.Getenv("S5CMD_GOPS") != "" {
		if err := agent.Listen(&agent.Options{NoShutdownCleanup: true}); err != nil {
			log.Fatal("-ERR", err)
		}
	}

	if *flagShowVersion {
		fmt.Printf("s5cmd version %s", GitSummary)
		if GitBranch != "" {
			fmt.Printf(" (from branch %s)", GitBranch)
		}
		fmt.Print("\n")
		os.Exit(0)
	}

	if flag.Arg(0) == "" && *flagCommandFile == "" {
		flag.Usage()
		os.Exit(2)
	}

	cmd := strings.Join(flag.Args(), " ")
	if cmd != "" && *flagCommandFile != "" {
		log.Fatal("-ERR Only specify -f or command, not both")
	}
	if (cmd == "" && *flagCommandFile == "") || *flagWorkerCount == 0 || *flagUploadPartSize < 1 || *flagRetryCount < 0 {
		log.Fatal("-ERR Please specify all arguments.")
	}

	ulPartSizeBytes := int64(*flagUploadPartSize * int(bytesInMb))
	if ulPartSizeBytes < int64(minUploadPartSize) {
		log.Fatalf("-ERR Multipart chunk size should be greater than %d", int(math.Ceil(minUploadPartSize/bytesInMb)))
	}
	dlPartSizeBytes := int64(*flagDownloadPartSize * int(bytesInMb))
	if dlPartSizeBytes < int64(5*bytesInMb) {
		log.Fatalf("-ERR Download part size should be greater than 5")
	}
	if *flagDownloadConcurrency < 1 || *flagUploadConcurrency < 1 {
		log.Fatalf("-ERR Download/Upload concurrency should be greater than 1")
	}

	var cmdMode bool
	if cmd != "" {
		cmdMode = true
	}

	if *flagWorkerCount < 0 {
		*flagWorkerCount = runtime.NumCPU() * -*flagWorkerCount
	}
	if *flagWorkerCount < minNumWorkers {
		*flagWorkerCount = minNumWorkers
	}

	*flagEndpointURL = strings.TrimSpace(*flagEndpointURL)

	startTime := time.Now()

	if !cmdMode {
		log.Printf("# Using %d workers", *flagWorkerCount)
	}

	parentCtx, cancelFunc := context.WithCancel(context.Background())

	exitCode := -1
	exitFunc := func(code int) {
		exitCode = code
		cancelFunc()
	}

	ctx := context.WithValue(
		context.WithValue(
			parentCtx,
			core.ExitFuncKey,
			exitFunc,
		),
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

	s := stats.Stats{}

	core.Verbose = *flagVerbose

	wp := core.NewWorkerPool(ctx,
		&core.WorkerPoolParams{
			NumWorkers:             *flagWorkerCount,
			UploadChunkSizeBytes:   ulPartSizeBytes,
			UploadConcurrency:      *flagUploadConcurrency,
			DownloadChunkSizeBytes: dlPartSizeBytes,
			DownloadConcurrency:    *flagDownloadConcurrency,
			Retries:                *flagRetryCount,
			EndpointURL:            *flagEndpointURL,
			NoVerifySSL:            *flagNoVerifySSL,
		}, &s)
	if cmdMode {
		wp.RunCmd(cmd)
	} else {
		wp.Run(*flagCommandFile)
	}

	elapsed := time.Since(startTime)

	failops := s.Get(stats.Fail)

	// if exitCode is -1 (default) and if we have at least one absolute-fail,
	// exit with code 127
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

	if !cmdMode || *flagPrintStats {
		s3ops := s.Get(stats.S3Op)
		fileops := s.Get(stats.FileOp)
		shellops := s.Get(stats.ShellOp)
		printOps("S3", s3ops, elapsed, "")
		printOps("File", fileops, elapsed, "")
		printOps("Shell", shellops, elapsed, "")
		printOps("Failed", failops, elapsed, "")

		printOps("Total", s3ops+fileops+shellops+failops, elapsed, fmt.Sprintf(" %v", elapsed))
	}

	os.Exit(exitCode)
}
