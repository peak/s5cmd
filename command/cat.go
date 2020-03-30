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

var catCommandFlags = []cli.Flag{
	&cli.IntFlag{
		Name:    "part-size",
		Aliases: []string{"p"},
		Value:   defaultPartSize,
		Usage:   "size of parts transferred between host and remote server, in MiB",
	},
}

var CatCommand = &cli.Command{
	Name:               "cat",
	HelpName:           "cat",
	Usage:              "print S3 object's contents to stdout",
	Flags:              catCommandFlags,
	CustomHelpTemplate: catHelpTemplate,
	Before: func(c *cli.Context) error {
		if c.Args().Len() != 1 {
			return fmt.Errorf("expected only one argument")
		}

		src, err := url.New(c.Args().Get(0))
		if err != nil {
			return err
		}

		if src.HasGlob() {
			return fmt.Errorf("remote source %q can not contain glob characters", src)
		}

		return nil
	},
	Action: func(c *cli.Context) error {
		src, err := url.New(c.Args().Get(0))
		if err != nil {
			return err
		}

		return Cat(c.Context, src, c.Int64("part-size")*megabytes)
	},
}

func Cat(ctx context.Context, src *url.URL, partSize int64) error {
	client, err := storage.NewClient(src)
	if err != nil {
		return err
	}
	_, err = client.Get(ctx, src, sequentialWriterAt{w: os.Stdout}, 1, partSize)
	if err != nil {
		return fmt.Errorf("get returned with: %w", err)
	}

	return nil
}

type sequentialWriterAt struct {
	w io.Writer
}

func (fw sequentialWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	// ignore 'offset' because we forced sequential downloads
	return fw.w.Write(p)
}
