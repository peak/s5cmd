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
`

var catCommand = &cli.Command{
	Name:               "cat",
	HelpName:           "cat",
	Usage:              "print remote object's contents to stdout",
	CustomHelpTemplate: catHelpTemplate,
	Before: func(c *cli.Context) error {
		err := validateCatCommand(c)
		if err != nil {
			printError(givenCommand(c), c.Command.Name, err)
		}
		return err
	},
	Action: func(c *cli.Context) (err error) {
		defer stat.Collect(c.Command.FullName(), &err)()

		src, err := url.New(c.Args().Get(0))
		op := c.Command.Name
		fullCommand := givenCommand(c)
		if err != nil {
			printError(fullCommand, op, err)
			return err
		}

		return Cat{
			src:         src,
			op:          op,
			fullCommand: fullCommand,

			storageOpts: storage.Options{
				MaxRetries:  c.Int("retry-count"),
				Endpoint:    c.String("endpoint-url"),
				NoVerifySSL: c.Bool("no-verify-ssl"),
				DryRun:      c.Bool("dry-run"),
			},
		}.Run(c.Context)
	},
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
	client, err := storage.NewClient(c.src, c.storageOpts)
	if err != nil {
		printError(c.fullCommand, c.op, err)
		return err
	}

	r, err := client.Open(ctx, c.src)
	if err != nil {
		printError(c.fullCommand, c.op, err)
		return err
	}

	rc, err := r.ReadCloser()
	if err != nil {
		return err
	}
	defer rc.Close()

	_, err = io.Copy(os.Stdout, rc)
	return err
}

func validateCatCommand(c *cli.Context) error {
	if c.Args().Len() != 1 {
		return fmt.Errorf("expected only one argument")
	}

	src, err := url.New(c.Args().Get(0))

	if err != nil {
		return err
	}

	if !src.IsRemote() {
		return fmt.Errorf("source must be a remote object")
	}

	if src.IsBucket() || src.IsPrefix() {
		return fmt.Errorf("remote source must be an object")
	}

	if src.HasGlob() {
		return fmt.Errorf("remote source %q can not contain glob characters", src)
	}
	return nil
}
