package command

import (
	"context"
	"fmt"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/v2/log/stat"
	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
)

var presignHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] source

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Print a remote object url to stdout
		 > s5cmd {{.HelpName}} s3://bucket/prefix/object

	2. Print a remote object url with a specific expiration time to stdout
		 > s5cmd {{.HelpName}} --expire 24h s3://bucket/prefix/object
`

func NewPresignCommand() *cli.Command {
	cmd := &cli.Command{
		Name:     "presign",
		HelpName: "presign",
		Usage:    "print remote object presign url",
		Flags: []cli.Flag{
			&cli.DurationFlag{
				Name:  "expire",
				Usage: "url valid duration",
				Value: time.Hour * 3,
			},
			&cli.StringFlag{
				Name:  "version-id",
				Usage: "use the specified version of an object",
			},
		},
		CustomHelpTemplate: presignHelpTemplate,
		Before: func(c *cli.Context) error {
			err := validatePresignCommand(c)
			if err != nil {
				printError(commandFromContext(c), c.Command.Name, err)
			}
			return err
		},
		Action: func(c *cli.Context) (err error) {
			defer stat.Collect(c.Command.FullName(), &err)()

			op := c.Command.Name
			fullCommand := commandFromContext(c)

			src, err := url.New(c.Args().Get(0), url.WithVersion(c.String("version-id")))
			if err != nil {
				printError(fullCommand, op, err)
				return err
			}

			return Presign{
				src:         src,
				op:          op,
				fullCommand: fullCommand,
				expire:      c.Duration("expire"),
				storageOpts: NewStorageOpts(c),
			}.Run(c.Context)
		},
	}
	return cmd
}

// Presign holds presign operation flags and states.
type Presign struct {
	src         *url.URL
	op          string
	fullCommand string
	expire      time.Duration

	storageOpts storage.Options
}

// Run prints content of given source to standard output.
func (c Presign) Run(ctx context.Context) error {
	client, err := storage.NewRemoteClient(ctx, c.src, c.storageOpts)
	if err != nil {
		printError(c.fullCommand, c.op, err)
		return err
	}

	url, err := client.Presign(ctx, c.src, c.expire)
	if err != nil {
		printError(c.fullCommand, c.op, err)
		return err
	}
	fmt.Println(url)
	return nil
}

func validatePresignCommand(c *cli.Context) error {
	if c.Args().Len() != 1 {
		return fmt.Errorf("expected remote object url")
	}

	src, err := url.New(c.Args().Get(0), url.WithVersion(c.String("version-id")))
	if err != nil {
		return err
	}

	if !src.IsRemote() {
		return fmt.Errorf("source must be a remote object")
	}

	if src.IsBucket() || src.IsPrefix() {
		return fmt.Errorf("remote source must be an object")
	}

	if src.IsWildcard() {
		return fmt.Errorf("remote source %q can not contain glob characters", src)
	}

	if err := checkVersioningWithGoogleEndpoint(c); err != nil {
		return err
	}

	return nil
}
