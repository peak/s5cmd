//go:generate go run version/cmd/generate.go

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/core"
	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/storage"
)

const (
	defaultWorkerCount         = 256
	defaultUploadConcurrency   = 5
	defaultDownloadConcurrency = 5
	defaultChunkSize           = 50
	minNumWorkers              = 2
	defaultRetryCount          = 10

	megabytes = 1024 * 1024
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		cancel()
	}()

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "download-concurrency", Aliases: []string{"dw"}, Value: defaultDownloadConcurrency},
			&cli.IntFlag{Name: "upload-concurrency", Aliases: []string{"uw"}, Value: defaultUploadConcurrency},
			&cli.IntFlag{Name: "download-chunk-size", Aliases: []string{"ds"}, Value: defaultChunkSize},
			&cli.IntFlag{Name: "upload-chunk-size", Aliases: []string{"us"}, Value: defaultChunkSize},
			&cli.IntFlag{Name: "retry-count", Aliases: []string{"r"}, Value: defaultRetryCount},
			&cli.BoolFlag{Name: "no-verify-ssl"},
			&cli.StringFlag{Name: "endpoint-url"},
			&cli.IntFlag{Name: "numworkers", Value: defaultWorkerCount},
			&cli.BoolFlag{Name: "version"},
			&cli.BoolFlag{Name: "json"},
			&cli.StringFlag{Name: "log", Value: "info"},
		},
		Before: func(c *cli.Context) error {
			downloadConcurrency := c.Int("download-concurrency")
			downloadChunkSize := c.Int64("download-chunk-size")
			uploadConcurrency := c.Int("upload-concurrency")
			uploadChunkSize := c.Int64("upload-chunk-size")
			noVerifySSL := c.Bool("no-verify-ssl")
			retryCount := c.Int("retry-count")
			endpointURL := c.String("endpoint-url")
			workerCount := c.Int("numworkers")
			printJSON := c.Bool("json")
			logLevel := c.String("log")

			// validation
			{
				if uploadChunkSize < 5 {
					return fmt.Errorf("upload chunk size should be greater than 5 MB")
				}

				if downloadChunkSize < 5 {
					return fmt.Errorf("download chunk size should be greater than 5 MB")
				}

				if downloadConcurrency < 1 || uploadConcurrency < 1 {
					return fmt.Errorf("download/upload concurrency should be greater than 1")
				}

				if retryCount < 1 {
					return fmt.Errorf("retry count must be a positive value")
				}
			}

			if workerCount < 0 {
				workerCount = runtime.NumCPU() * -workerCount
			}

			if workerCount < minNumWorkers {
				workerCount = minNumWorkers
			}

			s3opts := storage.S3Opts{
				MaxRetries:             retryCount,
				EndpointURL:            endpointURL,
				NoVerifySSL:            noVerifySSL,
				UploadChunkSizeBytes:   uploadChunkSize * megabytes,
				UploadConcurrency:      uploadConcurrency,
				DownloadChunkSizeBytes: downloadChunkSize * megabytes,
				DownloadConcurrency:    downloadConcurrency,
			}

			storage.SetS3Options(s3opts)

			log.Init(logLevel, printJSON)

			return nil
		},
		After: func(c *cli.Context) error {
			defer log.Close()
			return nil
		},
		Action: func(c *cli.Context) error {
			return cli.ShowAppHelp(c)
		},
	}

	app.Commands = []*cli.Command{
		core.ListCommand,
		core.SizeCommand,
		core.MakeBucketCommand,
		core.DeleteCommand,
		core.CopyCommand,
		core.VersionCommand,
	}

	if err := app.RunContext(ctx, os.Args); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
