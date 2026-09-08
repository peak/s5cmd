package command

import (
	"context"
	"fmt"
	"strings"

	"github.com/urfave/cli/v3"

	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
	"github.com/peak/s5cmd/v2/strutil"
)

var bucketVersionHelpTemplate = `Name:
	{{.FullName}} - {{.Usage}}

Usage:
	{{.FullName}} [options] s3://bucketname

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Get bucket versioning status of a bucket
		 > {{.FullName}} s3://bucketname

	2. Enable bucket versioning for the bucket
		 > {{.FullName}} --set Enabled s3://bucketname

	3. Suspend bucket versioning for the bucket
		 > {{.FullName}} --set Suspended s3://bucketname
`

func NewBucketVersionCommand() *cli.Command {
	cmd := &cli.Command{
		Name:               "bucket-version",
		CustomHelpTemplate: bucketVersionHelpTemplate,
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
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			if err := checkNumberOfArguments(c, 1, 1); err != nil {
				printError(commandFromContext(c), c.Name, err)
				return ctx, err
			}
			return ctx, nil
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			status := genericString(c, "set")

			fullCommand := commandFromContext(c)

			bucket, err := url.New(c.Args().First())
			if err != nil {
				printError(fullCommand, c.Name, err)
				return err
			}

			return BucketVersion{
				src:         bucket,
				op:          c.Name,
				fullCommand: fullCommand,

				status:      status,
				storageOpts: NewStorageOpts(c),
			}.Run(ctx)
		},
	}
	cmd.ShellComplete = getBashCompleteFn(cmd, true, true)
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
