package command

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
	"github.com/peak/s5cmd/strutil"
	"github.com/peak/s5cmd/version"
)

var versionHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] s3://bucketname

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Check the current version of the s5cmd
	 	 > s5cmd {{.HelpName}}
	
	2. Get the bucket versioning status of a bucket
		 > s5cmd {{.HelpName}} --get s3://bucketname

	3. Enable bucket versioning for the bucket
		 > s5cmd {{.HelpName}} --set Enabled s3://bucketname

	4. Suspend bucket versioning for the bucket
		 > s5cmd {{.HelpName}} --set Suspended s3://bucketname
`

func NewVersionCommand() *cli.Command {
	return &cli.Command{
		Name:               "version",
		CustomHelpTemplate: versionHelpTemplate,
		HelpName:           "version",
		Usage:              "print version",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name: "set",
				// todo use generic flag when https://github.com/urfave/cli/issues/1441
				// solved
				// Value: &EnumValue{
				// 	Enum:    []string{"Suspended", "Enabled"},
				// 	Default: "",
				// },
				Usage: "set versioning status of bucket: (Suspended, Enabled)",
			},
			&cli.BoolFlag{
				Name:  "get",
				Usage: "get versioning status of bucket",
			},
		},
		Before: func(c *cli.Context) error {
			// todo validate commmand
			// check if the status  argument is valid
			// to be handled by using GenericFlags  & Enum values
			status := c.String("set")
			if c.IsSet("set") && status != "Suspended" && status != "Enabled" {
				errMessage := "Incorrect Usage: invalid value \"" + status + "\" for flag --set: allowed values: [Suspended, Enabled]"
				fmt.Println(errMessage)
				return fmt.Errorf(errMessage)
			}
			return nil
		},
		Action: func(c *cli.Context) error {
			status := c.String("set")
			if status == "" && !c.Bool("get") {
				fmt.Println(version.GetHumanVersion())
				return nil
			}

			fullCommand := commandFromContext(c)

			bucket, err := url.New(c.Args().First())
			if err != nil {
				printError(fullCommand, c.Command.Name, err)
				return err
			}

			return Versioning{
				src:         bucket,
				op:          c.Command.Name,
				fullCommand: fullCommand,

				status:      status,
				storageOpts: NewStorageOpts(c),
			}.Run(c.Context)
		},
	}
}

type Versioning struct {
	src         *url.URL
	op          string
	fullCommand string

	status      string
	storageOpts storage.Options
}

func (v Versioning) Run(ctx context.Context) error {
	client, err := storage.NewRemoteClient(ctx, &url.URL{}, v.storageOpts)
	if err != nil {
		printError(v.fullCommand, v.op, err)
		return err
	}

	if v.status != "" {
		// check if the status  argument is valid
		// to be handled by using GenericFlags  & Enum values
		if v.status != "Suspended" && v.status != "Enabled" {
			errMessage := "Incorrect Usage: invalid value \"" + v.status + "\" for flag --set: allowed values: [Suspended, Enabled]"
			fmt.Println(errMessage)
			return fmt.Errorf(errMessage)
		}
		err := client.SetBucketVersioning(ctx, v.status, v.src.Bucket)
		if err != nil {
			printError(v.fullCommand, v.op, err)
			return err
		}
		msg := VersioningMessage{
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

	msg := VersioningMessage{
		Bucket: v.src.Bucket,
		Status: status,
		isSet:  false,
	}
	log.Info(msg)
	return nil
}

type VersioningMessage struct {
	Bucket string `json:"bucket"`
	Status string `json:"status"`
	isSet  bool
}

func (v VersioningMessage) String() string {
	if v.isSet {
		return fmt.Sprintf("Bucket versioning for %q is set to %q", v.Bucket, v.Status)
	}
	return fmt.Sprintf("Bucket versioning for %q is %q", v.Bucket, v.Status)
}

func (v VersioningMessage) JSON() string {
	return strutil.JSON(v)
}
