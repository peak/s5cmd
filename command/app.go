package command

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/parallel"
	"github.com/peak/s5cmd/storage"
)

const (
	defaultWorkerCount         = 256
	defaultUploadConcurrency   = 5
	defaultDownloadConcurrency = 5
	defaultChunkSize           = 50 // MiB
	defaultRetryCount          = 10

	megabytes = 1024 * 1024
)

var app = &cli.App{
	HideHelp: true,
	Flags: []cli.Flag{
		&cli.IntFlag{Name: "download-concurrency", Aliases: []string{"dw"}, Value: defaultDownloadConcurrency},
		&cli.IntFlag{Name: "upload-concurrency", Aliases: []string{"uw"}, Value: defaultUploadConcurrency},
		&cli.IntFlag{Name: "download-chunk-size", Aliases: []string{"ds"}, Value: defaultChunkSize},
		&cli.IntFlag{Name: "upload-chunk-size", Aliases: []string{"us"}, Value: defaultChunkSize},
		&cli.IntFlag{Name: "retry-count", Aliases: []string{"r"}, Value: defaultRetryCount},
		&cli.BoolFlag{Name: "no-verify-ssl"},
		&cli.StringFlag{Name: "endpoint-url"},
		&cli.IntFlag{Name: "numworkers", Value: defaultWorkerCount},
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

		parallel.Init(workerCount)

		return nil
	},
	Action: func(c *cli.Context) error {
		return cli.ShowAppHelp(c)
	},
	After: func(c *cli.Context) error {
		parallel.Close()
		log.Close()
		return nil
	},
	ExitErrHandler: func(c *cli.Context, err error) {
		if err != nil {
			printError(givenCommand(c), c.Command.Name, err)
		}
	},
}

func Main(ctx context.Context, args []string) error {
	app.Commands = []*cli.Command{
		ListCommand,
		SizeCommand,
		MakeBucketCommand,
		DeleteCommand,
		CopyCommand,
		MoveCommand,
		GetCommand,
		RunCommand,
		VersionCommand,
	}

	return app.RunContext(ctx, args)
}
