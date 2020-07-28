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
	},
	Action: func(c *cli.Context) error {
		return MakeBucket(
			c.Context,
			c.Command.Name,
			c.Args().First(),
			s3opts(c, true),
		)
	},
}

// MakeBucket creates bucket.
func MakeBucket(
	ctx context.Context,
	op string,
	src string,
	s3opts storage.S3Options,
) error {
	bucket, err := url.New(src)
	if err != nil {
		return err
	}

	client, err := storage.NewClient(bucket, s3opts)
	if err != nil {
		return err
	}

	err = client.MakeBucket(ctx, bucket.Bucket)
	if err != nil {
		return err
	}

	msg := log.InfoMessage{
		Operation: op,
		Source:    bucket,
	}
	log.Info(msg)

	return nil
}
