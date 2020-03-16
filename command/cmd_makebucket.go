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
	Usage:    "creates a bucket",
	Before: func(c *cli.Context) error {
		if c.Args().Len() != 1 {
			return fmt.Errorf("expected only 1 argument")
		}

		src := c.Args().First()
		bucket, err := objurl.New(src)
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
		)
	},
}

func MakeBucket(
	ctx context.Context,
	op string,
	src string,
) error {
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
		Operation: op,
		Source:    bucket,
	}
	log.Info(msg)

	return nil
}
