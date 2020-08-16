package command

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/urfave/cli/v2"

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
	Action: func(c *cli.Context) error {
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
		}.Run(c.Context)
	},
}

// Cat holds cat operation flags and states.
type Cat struct {
	src         *url.URL
	op          string
	fullCommand string
}

// Run prints content of given source to standard output.
func (c Cat) Run(ctx context.Context) error {
	client, err := storage.NewClient(c.src)
	if err != nil {
		return err
	}

	// set concurrency to 1 for sequential write to 'stdout' and give a dummy 'partSize' since
	// `storage.S3.Get()` ignores 'partSize' if concurrency is set to 1.
	_, err = client.Get(ctx, c.src, sequentialWriterAt{w: os.Stdout}, 1, -1)
	if err != nil {
		printError(c.fullCommand, c.op, err)
		return err
	}
	return nil
}

type sequentialWriterAt struct {
	w io.Writer
}

func (sw sequentialWriterAt) WriteAt(p []byte, offset int64) (int, error) {
	// ignore 'offset' because we forced sequential downloads
	return sw.w.Write(p)
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
