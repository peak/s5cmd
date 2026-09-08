package command

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v3"

	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/log/stat"
	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
)

var makeBucketHelpTemplate = `Name:
	{{.FullName}} - {{.Usage}}

Usage:
	{{.FullName}} s3://bucketname

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Create a new S3 bucket
		 > {{.FullName}} s3://bucketname
`

func NewMakeBucketCommand() *cli.Command {
	cmd := &cli.Command{
		Name:               "mb",
		Usage:              "make bucket",
		CustomHelpTemplate: makeBucketHelpTemplate,
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			err := validateMBCommand(c)
			if err != nil {
				printError(commandFromContext(c), c.Name, err)
			}
			return ctx, err
		},
		Action: func(ctx context.Context, c *cli.Command) (err error) {
			defer stat.Collect(commandName(c), &err)()

			return MakeBucket{
				src:         c.Args().First(),
				op:          c.Name,
				fullCommand: commandFromContext(c),

				storageOpts: NewStorageOpts(c),
			}.Run(ctx)
		},
	}
	cmd.ShellComplete = func(ctx context.Context, c *cli.Command) {
		arg := parseArgumentToComplete(c)
		if strings.HasPrefix(arg, "-") {
			cli.DefaultCompleteWithFlags(ctx, cmd)
		} else {
			shell := filepath.Base(os.Getenv("SHELL"))
			constantCompleteWithDefault(shell, arg, "s3://")
		}
	}

	return cmd
}

// MakeBucket holds bucket creation operation flags and states.
type MakeBucket struct {
	src         string
	op          string
	fullCommand string

	storageOpts storage.Options
}

// Run creates a bucket.
func (b MakeBucket) Run(ctx context.Context) error {
	bucket, err := url.New(b.src)
	if err != nil {
		printError(b.fullCommand, b.op, err)
		return err
	}

	client, err := storage.NewRemoteClient(ctx, &url.URL{}, b.storageOpts)
	if err != nil {
		printError(b.fullCommand, b.op, err)
		return err
	}

	if err := client.MakeBucket(ctx, bucket.Bucket); err != nil {
		printError(b.fullCommand, b.op, err)
		return err
	}

	msg := log.InfoMessage{
		Operation: b.op,
		Source:    bucket,
	}
	log.Info(msg)

	return nil
}

func validateMBCommand(c *cli.Command) error {
	if c.Args().Len() != 1 {
		return fmt.Errorf("expected only 1 argument")
	}

	src := c.Args().First()
	bucket, err := url.New(src)
	if err != nil {
		return err
	}
	if !bucket.IsBucket() {
		return fmt.Errorf("invalid s3 bucket")
	}

	return nil
}
