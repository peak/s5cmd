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
		fullCommand := givenCommand(c)
		op := c.Command.Name
		if c.Args().Len() != 1 {
			err := fmt.Errorf("expected only 1 argument")
			printError(fullCommand, op, err)
		}

		src := c.Args().First()
		bucket, err := url.New(src)
		if err != nil {
			printError(fullCommand, op, err)
			return err
		}
		if !bucket.IsBucket() {
			err := fmt.Errorf("invalid s3 bucket")
			printError(fullCommand, op, err)
			return err
		}

		return nil
	},
	Action: func(c *cli.Context) error {
		return MakeBucket(
			c.Context,
			c.Command.Name,
			givenCommand(c),
			c.Args().First(),
		)
	},
}

// MakeBucket creates bucket.
func MakeBucket(
	ctx context.Context,
	op string,
	fullCommand string,
	src string,
) error {
	bucket, err := url.New(src)
	if err != nil {
		printError(fullCommand, op, err)
		return err
	}

	client := storage.NewClient(bucket)

	err = client.MakeBucket(ctx, bucket.Bucket)
	if err != nil {
		printError(fullCommand, op, err)
		return err
	}

	msg := log.InfoMessage{
		Operation: op,
		Source:    bucket,
	}
	log.Info(msg)

	return nil
}
