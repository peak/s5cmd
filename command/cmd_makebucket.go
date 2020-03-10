package command

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/storage"
)

var MakeBucketCommand = &cli.Command{
	Name:     "mb",
	HelpName: "make-bucket",
	Usage:    "TODO",
	Before: func(c *cli.Context) error {
		if c.Args().Len() != 1 {
			return fmt.Errorf("expected only 1 argument")
		}
		return nil
	},
	OnUsageError: func(c *cli.Context, err error, isSubcommand bool) error {
		if err != nil {
			printError(givenCommand(c), "make-bucket", err)
		}
		return err
	},
	Action: func(c *cli.Context) error {
		return MakeBucket(
			c.Context,
			givenCommand(c),
			c.Args().First(),
		)
	},
}

func MakeBucket(ctx context.Context, fullCommand string, src string) error {
	bucket, err := objurl.New(src)
	if err != nil {
		return err
	}

	client, err := storage.NewClient(bucket)
	if err != nil {
		return err
	}

	err = client.MakeBucket(ctx, bucket.Bucket)
	if err != nil {
		return err
	}

	msg := log.InfoMessage{
		Operation: "make-bucket",
		Source:    bucket,
	}
	log.Info(msg)

	return nil
}
