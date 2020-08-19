package command

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
)

var makeBucketHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} bucketname

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Create a new S3 bucket
		 > s5cmd {{.HelpName}} newbucket
`

var makeBucketCommand = &cli.Command{
	Name:               "mb",
	HelpName:           "mb",
	Usage:              "make bucket",
	CustomHelpTemplate: makeBucketHelpTemplate,
	Before: func(c *cli.Context) error {
		err := validateMBCommand(c)
		if err != nil {
			printError(givenCommand(c), c.Command.Name, err)
		}
		return err
	},
	Action: func(c *cli.Context) error {

		return Bucket{
			src:         c.Args().First(),
			op:          c.Command.Name,
			fullCommand: givenCommand(c),

			storageOpts: storage.Options{
				MaxRetries:  c.Int("retry-count"),
				Endpoint:    c.String("endpoint-url"),
				NoVerifySSL: c.Bool("no-verify-ssl"),
				DryRun:      c.Bool("dry-run"),
			},
		}.Run(c.Context)
	},
}

type Bucket struct {
	src         string
	op          string
	fullCommand string

	storageOpts storage.Options
}

// Run creates bucket.

func (b Bucket) Run(ctx context.Context) error {
	bucket, err := url.New(b.src)
	if err != nil {
		printError(b.fullCommand, b.op, err)
		return err
	}

	client, err := storage.NewClient(bucket, b.storageOpts)
	if err != nil {
		printError(b.fullCommand, b.op, err)
		return err
	}

	_, err = client.Make(ctx, storage.MakeOpts{
		Path: bucket.Bucket,
	})
	if err != nil {
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

func validateMBCommand(c *cli.Context) error {
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
