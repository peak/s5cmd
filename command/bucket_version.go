package command

import (
	"context"
	"fmt"
	"strings"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
	"github.com/peak/s5cmd/strutil"
)

var bucketVersionHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] s3://bucketname

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Get bucket versioning status of a bucket
		 > s5cmd {{.HelpName}} s3://bucketname

	2. Enable bucket versioning for the bucket
		 > s5cmd {{.HelpName}} --set Enabled s3://bucketname

	3. Suspend bucket versioning for the bucket
		 > s5cmd {{.HelpName}} --set Suspended s3://bucketname
`

func NewBucketVersionCommand() *cli.Command {
	cmd := &cli.Command{
		Name:               "bucket-version",
		CustomHelpTemplate: bucketVersionHelpTemplate,
		HelpName:           "bucket-version",
		Usage:              "configure bucket versioning",
		Flags: []cli.Flag{
			&cli.GenericFlag{
				Name: "set",
				Value: &EnumValue{
					Enum:              []string{"Suspended", "Enabled"},
					Default:           "",
					ConditionFunction: strings.EqualFold,
				},
				Usage: "set versioning status of bucket: (Suspended, Enabled)",
			},
		},
		Before: func(ctx *cli.Context) error {
			if err := checkNumberOfArguments(ctx, 1, 1); err != nil {
				printError(commandFromContext(ctx), ctx.Command.Name, err)
				return err
			}
			return nil
		},
		Action: func(c *cli.Context) error {
			status := c.String("set")

			fullCommand := commandFromContext(c)

			bucket, err := url.New(c.Args().First())
			if err != nil {
				printError(fullCommand, c.Command.Name, err)
				return err
			}

			return BucketVersion{
				src:         bucket,
				op:          c.Command.Name,
				fullCommand: fullCommand,

				status:      status,
				storageOpts: NewStorageOpts(c),
			}.Run(c.Context)
		},
	}
	cmd.BashComplete = getBashCompleteFn(cmd, true, true)
	return cmd
}

type BucketVersion struct {
	src         *url.URL
	op          string
	fullCommand string

	status      string
	storageOpts storage.Options
}

func (v BucketVersion) Run(ctx context.Context) error {
	client, err := storage.NewRemoteClient(ctx, &url.URL{}, v.storageOpts)
	if err != nil {
		printError(v.fullCommand, v.op, err)
		return err
	}

	if v.status != "" {
		v.status = strutil.CapitalizeFirstRune(v.status)

		err := client.SetBucketVersioning(ctx, v.status, v.src.Bucket)
		if err != nil {
			printError(v.fullCommand, v.op, err)
			return err
		}
		msg := BucketVersionMessage{
			Bucket: v.src.Bucket,
			Status: v.status,
			isSet:  true,
		}
		log.Info(msg)
		return nil
	}

	status, err := client.GetBucketVersioning(ctx, v.src.Bucket)
	if err != nil {
		printError(v.fullCommand, v.op, err)
		return err
	}

	msg := BucketVersionMessage{
		Bucket: v.src.Bucket,
		Status: status,
		isSet:  false,
	}
	log.Info(msg)
	return nil
}

type BucketVersionMessage struct {
	Bucket string `json:"bucket"`
	Status string `json:"status"`
	isSet  bool
}

func (v BucketVersionMessage) String() string {
	if v.isSet {
		return fmt.Sprintf("Bucket versioning for %q is set to %q", v.Bucket, v.Status)
	}
	if v.Status != "" {
		return fmt.Sprintf("Bucket versioning for %q is %q", v.Bucket, v.Status)
	}
	return fmt.Sprintf("%q is an unversioned bucket", v.Bucket)
}

func (v BucketVersionMessage) JSON() string {
	return strutil.JSON(v)
}
