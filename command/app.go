package command

import (
	"context"
	"fmt"

	cmpinstall "github.com/posener/complete/cmd/install"
	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/parallel"
	"github.com/peak/s5cmd/storage"
)

const (
	defaultWorkerCount = 256
	defaultRetryCount  = 10

	appName = "s5cmd"
)

var app = &cli.App{
	Name:  appName,
	Usage: "Blazing fast S3 and local filesystem execution tool",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "json",
			Usage: "enable JSON formatted output",
		},
		&cli.IntFlag{
			Name:  "numworkers",
			Value: defaultWorkerCount,
			Usage: "number of workers execute operation on each object",
		},
		&cli.IntFlag{
			Name:    "retry-count",
			Aliases: []string{"r"},
			Value:   defaultRetryCount,
			Usage:   "number of times that a request will be retried for failures",
		},
		&cli.StringFlag{
			Name:  "endpoint-url",
			Usage: "override default S3 host for custom services",
		},
		&cli.BoolFlag{
			Name:  "no-verify-ssl",
			Usage: "disable SSL certificate verification",
		},
		&cli.StringFlag{
			Name:  "log",
			Value: "info",
			Usage: "log level: (verbose, info, error)",
		},
		&cli.BoolFlag{
			Name:  "install-completion",
			Usage: "install completion for your shell",
		},
	},
	Before: func(c *cli.Context) error {
		noVerifySSL := c.Bool("no-verify-ssl")
		retryCount := c.Int("retry-count")
		endpointURL := c.String("endpoint-url")
		workerCount := c.Int("numworkers")
		printJSON := c.Bool("json")
		logLevel := c.String("log")

		log.Init(logLevel, printJSON)
		parallel.Init(workerCount)

		if retryCount < 0 {
			return fmt.Errorf("retry count cannot be a negative value")
		}

		s3opts := storage.S3Options{
			MaxRetries:  retryCount,
			Endpoint:    endpointURL,
			NoVerifySSL: noVerifySSL,
		}

		storage.SetS3Options(s3opts)

		return nil
	},
	Action: func(c *cli.Context) error {
		if c.Bool("install-completion") {
			if cmpinstall.IsInstalled(appName) {
				return nil
			}

			return cmpinstall.Install(appName)
		}

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
		CopyCommand,
		DeleteCommand,
		MoveCommand,
		MakeBucketCommand,
		SizeCommand,
		RunCommand,
		VersionCommand,
	}

	if autoComplete() {
		return nil
	}

	return app.RunContext(ctx, args)
}
