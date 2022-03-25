package command

import (
	"context"
	"fmt"

	cmpinstall "github.com/posener/complete/cmd/install"
	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/log/stat"
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
			Name:    "endpoint-url",
			Usage:   "override default S3 host for custom services",
			EnvVars: []string{"S3_ENDPOINT_URL"},
		},
		&cli.BoolFlag{
			Name:  "no-verify-ssl",
			Usage: "disable SSL certificate verification",
		},
		&cli.StringFlag{
			Name:  "log",
			Value: "info",
			Usage: "log level: (debug, info, error)",
		},
		&cli.BoolFlag{
			Name:  "install-completion",
			Usage: "install completion for your shell",
		},
		&cli.BoolFlag{
			Name:  "dry-run",
			Usage: "fake run; show what commands will be executed without actually executing them",
		},
		&cli.BoolFlag{
			Name:  "stat",
			Usage: "collect statistics of program execution and display it at the end",
		},
		&cli.BoolFlag{
			Name:  "no-sign-request",
			Usage: "do not sign requests: credentials will not be loaded if --no-sign-request is provided",
		},
		&cli.BoolFlag{
			Name:  "use-v1-api",
			Usage: "enable backward compatibility with ListObjects API and disable ListObjectsV2 API",
		},
	},
	Before: func(c *cli.Context) error {
		retryCount := c.Int("retry-count")
		workerCount := c.Int("numworkers")
		printJSON := c.Bool("json")
		logLevel := c.String("log")
		isStat := c.Bool("stat")

		log.Init(logLevel, printJSON)
		parallel.Init(workerCount)

		if retryCount < 0 {
			err := fmt.Errorf("retry count cannot be a negative value")
			printError(commandFromContext(c), c.Command.Name, err)
			return err
		}

		if isStat {
			stat.InitStat()
		}

		return nil
	},
	CommandNotFound: func(c *cli.Context, command string) {
		msg := log.ErrorMessage{
			Command: command,
			Err:     "command not found",
		}
		log.Error(msg)

		// After callback is not called if app exists with cli.Exit.
		parallel.Close()
		log.Close()
	},
	Action: func(c *cli.Context) error {
		if c.Bool("install-completion") {
			if cmpinstall.IsInstalled(appName) {
				return nil
			}

			return cmpinstall.Install(appName)
		}

		args := c.Args()
		if args.Present() {
			cli.ShowCommandHelp(c, args.First())
			return cli.Exit("", 1)
		}

		return cli.ShowAppHelp(c)
	},
	After: func(c *cli.Context) error {
		if c.Bool("stat") {
			log.Info(stat.Statistics())
		}

		parallel.Close()
		log.Close()
		return nil
	},
}

// NewStorageOpts creates storage.Options object from the given context.
func NewStorageOpts(c *cli.Context) storage.Options {
	return storage.Options{
		MaxRetries:    c.Int("retry-count"),
		Endpoint:      c.String("endpoint-url"),
		NoVerifySSL:   c.Bool("no-verify-ssl"),
		DryRun:        c.Bool("dry-run"),
		NoSignRequest: c.Bool("no-sign-request"),
		APIv1Enabled:  c.Bool("use-v1-api"),
	}
}

func Commands() []*cli.Command {
	return []*cli.Command{
		NewListCommand(),
		NewCopyCommand(),
		NewDeleteCommand(),
		NewMoveCommand(),
		NewMakeBucketCommand(),
		NewRemoveBucketCommand(),
		NewSelectCommand(),
		NewSizeCommand(),
		NewCatCommand(),
		NewRunCommand(),
		NewSyncCommand(),
		NewVersionCommand(),
	}
}

func AppCommand(name string) *cli.Command {
	for _, c := range Commands() {
		if c.HasName(name) {
			return c
		}
	}

	return nil
}

// Main is the entrypoint function to run given commands.
func Main(ctx context.Context, args []string) error {
	app.Commands = Commands()

	if maybeAutoComplete() {
		return nil
	}

	return app.RunContext(ctx, args)
}
