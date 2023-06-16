package command

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/log/stat"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
)

var catHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] source

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Print a remote object's content to stdout
		 > s5cmd {{.HelpName}} s3://bucket/prefix/object

	2. Print specific version of a remote object's content to stdout
		 > s5cmd {{.HelpName}} --version-id VERSION_ID s3://bucket/prefix/object
`

func NewCatCommand() *cli.Command {
	cmd := &cli.Command{
		Name:     "cat",
		HelpName: "cat",
		Usage:    "print remote object content",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "raw",
				Usage: "disable the wildcard operations, useful with filenames that contains glob characters",
			},
			&cli.StringFlag{
				Name:  "version-id",
				Usage: "use the specified version of an object",
			},
		},
		CustomHelpTemplate: catHelpTemplate,
		Before: func(c *cli.Context) error {
			err := validateCatCommand(c)
			if err != nil {
				printError(commandFromContext(c), c.Command.Name, err)
			}
			return err
		},
		Action: func(c *cli.Context) (err error) {
			defer stat.Collect(c.Command.FullName(), &err)()

			op := c.Command.Name
			fullCommand := commandFromContext(c)

			src, err := url.New(c.Args().Get(0), url.WithVersion(c.String("version-id")),
				url.WithRaw(c.Bool("raw")))
			if err != nil {
				printError(fullCommand, op, err)
				return err
			}

			return Cat{
				src:         src,
				op:          op,
				fullCommand: fullCommand,

				storageOpts: NewStorageOpts(c),
			}.Run(c.Context)
		},
	}
	cmd.BashComplete = getBashCompleteFn(cmd, true, false)
	return cmd
}

// Cat holds cat operation flags and states.
type Cat struct {
	src         *url.URL
	op          string
	fullCommand string

	storageOpts storage.Options
}

// Run prints content of given source to standard output.
func (c Cat) Run(ctx context.Context) error {
	client, err := storage.NewRemoteClient(ctx, c.src, c.storageOpts)
	if err != nil {
		printError(c.fullCommand, c.op, err)
		return err
	}

	rc, err := client.Read(ctx, c.src)
	if err != nil {
		printError(c.fullCommand, c.op, err)
		return err
	}
	defer rc.Close()

	_, err = io.Copy(os.Stdout, rc)
	if err != nil {
		printError(c.fullCommand, c.op, err)
		return err
	}

	return nil
}

func validateCatCommand(c *cli.Context) error {
	if c.Args().Len() != 1 {
		return fmt.Errorf("expected only one argument")
	}

	src, err := url.New(c.Args().Get(0), url.WithVersion(c.String("version-id")),
		url.WithRaw(c.Bool("raw")))
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
