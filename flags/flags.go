package flags

import (
	"flag"
	"fmt"
	"math"
	"runtime"
	"strings"
)

const (
	defaultWorkerCount         = 256
	defaultUploadConcurrency   = 5
	defaultDownloadConcurrency = 5
	minNumWorkers              = 2
	minUploadPartSize          = 5 * megabytes

	megabytes = 1024 * 1024
)

var (
	CommandFile         = flag.String("f", "", "Commands-file or - for stdin")
	EndpointURL         = flag.String("endpoint-url", "", "Override default URL with the given one")
	WorkerCount         = flag.Int("numworkers", defaultWorkerCount, "Number of worker goroutines. Negative numbers mean multiples of the CPU core count")
	DownloadConcurrency = flag.Int("dw", defaultDownloadConcurrency, "Download concurrency for each file")
	DownloadPartSize    = flag.Int64("ds", 50, "Multipart chunk size in MB for downloads")
	UploadConcurrency   = flag.Int("uw", defaultUploadConcurrency, "Upload concurrency for each file")
	UploadPartSize      = flag.Int64("us", 50, "Multipart chunk size in MB for uploads")
	RetryCount          = flag.Int("r", 10, "Retry S3 operations N times before failing")
	PrintStats          = flag.Bool("stats", false, "Always print stats")
	ShowVersion         = flag.Bool("version", false, "Prints current version")
	EnableGops          = flag.Bool("gops", false, "Initialize gops agent")
	Verbose             = flag.Bool("vv", false, "Verbose output")
	NoVerifySSL         = flag.Bool("no-verify-ssl", false, "Don't verify SSL certificates")

	InstallCompletion   = flag.Bool("cmp-install", false, "Install shell completion")
	UninstallCompletion = flag.Bool("cmp-uninstall", false, "Uninstall shell completion")
)

func Parse() {
	flag.Parse()
}

func Validate() error {
	*UploadPartSize = *UploadPartSize * megabytes
	if *UploadPartSize < int64(minUploadPartSize) {
		minValue := int(math.Ceil(minUploadPartSize / float64(megabytes)))
		return fmt.Errorf("multipart chunk size should be greater than %v", minValue)
	}

	*DownloadPartSize = *DownloadPartSize * megabytes
	if *DownloadPartSize < 5*megabytes {
		return fmt.Errorf("download part size should be greater than 5")
	}

	if *DownloadConcurrency < 1 || *UploadConcurrency < 1 {
		return fmt.Errorf("download/upload concurrency should be greater than 1")
	}

	if flag.Arg(0) == "" && *CommandFile == "" {
		flag.Usage()
		return fmt.Errorf("no command file nor a command specified")
	}

	cmd := strings.Join(flag.Args(), " ")
	if cmd != "" && *CommandFile != "" {
		return fmt.Errorf("either specify -f or command, not both")
	}

	if *UploadPartSize < 1 {
		return fmt.Errorf("multipart chunk size for uploads must be a positive value")
	}

	if *DownloadPartSize < 1 {
		return fmt.Errorf("multipart chunk size for downloads must be a positive value")
	}

	if *RetryCount < 1 {
		return fmt.Errorf("retry count must be a positive value")
	}

	if *WorkerCount < 0 {
		*WorkerCount = runtime.NumCPU() * -*WorkerCount
	}

	if *WorkerCount < minNumWorkers {
		*WorkerCount = minNumWorkers
	}

	*EndpointURL = strings.TrimSpace(*EndpointURL)

	return nil
}
