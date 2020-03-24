package command

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/urfave/cli/v2"

	errorpkg "github.com/peak/s5cmd/error"
	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
)

var DeleteCommand = &cli.Command{
	Name:     "rm",
	HelpName: "rm",
	Usage:    "remove objects",
	Before: func(c *cli.Context) error {
		// TODO(ig): support variadic args
		if c.Args().Len() != 1 {
			return fmt.Errorf("expected 1 object to remove")
		}
		return nil
	},
	Action: func(c *cli.Context) error {
		return Delete(
			c.Context,
			c.Command.Name,
			givenCommand(c),
			c.Args().First(),
		)
	},
}

func Delete(
	ctx context.Context,
	op string,
	fullCommand string,
	src string,
) error {
	srcurl, err := url.New(src)
	if err != nil {
		return err
	}

	client, err := storage.NewClient(srcurl)
	if err != nil {
		return err
	}

	// storage.MultiDelete operates on file-like objects. Settings
	// recursive=true guarantees returning only file-like objects.
	objch, err := expandSource(ctx, srcurl, true)
	if err != nil {
		return err
	}

	// do object->url transformation
	urlch := make(chan *url.URL)
	go func() {
		defer close(urlch)

		for object := range objch {
			if object.Type.IsDir() || errorpkg.IsCancelation(object.Err) {
				continue
			}

			if err := object.Err; err != nil {
				printError(fullCommand, op, err)
				continue
			}
			urlch <- object.URL
		}
	}()

	resultch := client.MultiDelete(ctx, urlch)

	var merror error
	for obj := range resultch {
		if err := obj.Err; err != nil {
			if errorpkg.IsCancelation(obj.Err) {
				continue
			}

			merror = multierror.Append(merror, obj.Err)
			continue
		}

		msg := log.InfoMessage{
			Operation: op,
			Source:    obj.URL,
		}
		log.Info(msg)
	}

	return merror
}
