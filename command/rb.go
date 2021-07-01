package command

import (
	"context"
	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/log/stat"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
)

var removeBucketHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} bucketName

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Deletes S3 bucket with given name
		 > s5cmd {{.HelpName}} bucketName
`

var removeBucketCommand = &cli.Command{
	Name:               "rb",
	HelpName:           "rb",
	Usage:              "remove bucket",
	CustomHelpTemplate: removeBucketHelpTemplate,
	Before: func(c *cli.Context) error {
		err := validateMBCommand(c)
		if err != nil {
			printError(givenCommand(c), c.Command.Name, err)
		}
		return err
	},
	Action: func(c *cli.Context) (err error) {
		defer stat.Collect(c.Command.FullName(), &err)()

		return RemoveBucket{
			src:         c.Args().First(),
			op:          c.Command.Name,
			fullCommand: givenCommand(c),

			storageOpts: NewStorageOpts(c),
		}.Run(c.Context)
	},
}

// RemoveBucket holds bucket deletion operation flags and states.
type RemoveBucket struct {
	src         string
	op          string
	fullCommand string

	storageOpts storage.Options
}

// Run creates a bucket.
func (b RemoveBucket) Run(ctx context.Context) error {
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

	if err := client.RemoveBucket(ctx, bucket.Bucket); err != nil {
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

/*
func validateRBCommand(c *cli.Context) error {
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
*/
