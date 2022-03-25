package command

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/log/stat"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
)

var makeBucketHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} s3://bucketname

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Create a new S3 bucket
		 > s5cmd {{.HelpName}} s3://bucketname
`

func NewMakeBucketCommand() *cli.Command {
	return &cli.Command{
		Name:               "mb",
		HelpName:           "mb",
		Usage:              "make bucket",
		CustomHelpTemplate: makeBucketHelpTemplate,
		Before: func(c *cli.Context) error {
			err := validateMBCommand(c)
			if err != nil {
				printError(commandFromContext(c), c.Command.Name, err)
			}
			return err
		},
		Action: func(c *cli.Context) (err error) {
			defer stat.Collect(c.Command.FullName(), &err)()

			return MakeBucket{
				src:         c.Args().First(),
				op:          c.Command.Name,
				fullCommand: commandFromContext(c),

				storageOpts: NewStorageOpts(c),
			}.Run(c.Context)
		},
	}
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
