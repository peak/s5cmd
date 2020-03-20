package command

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/urfave/cli/v2"

	errorpkg "github.com/peak/s5cmd/error"
	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/storage"
)

var DeleteCommand = &cli.Command{
	Name:     "rm",
	HelpName: "rm",
	Usage:    "remove objects",
	Before: func(c *cli.Context) error {
		if !c.Args().Present() {
			return fmt.Errorf("expected at least 1 object to remove")
		}

		if err := checkSources(c.Args().Slice()...); err != nil {
			return err
		}

		return nil
	},
	Action: func(c *cli.Context) error {
		return Delete(
			c.Context,
			c.Command.Name,
			givenCommand(c),
			c.Args().Slice()...,
		)
	},
}

func Delete(
	ctx context.Context,
	op string,
	fullCommand string,
	src ...string,
) error {
	srcurls, err := newSources(src...)
	if err != nil {
		return err
	}
	srcurl := srcurls[0]

	client, err := storage.NewClient(srcurl)
	if err != nil {
		return err
	}

	// storage.MultiDelete operates on file-like objects. Settings
	// recursive=true guarantees returning only file-like objects.
	objChan, err := expandSources(ctx, true, nil, srcurls...)
	if err != nil {
		return err
	}

	// do object->objurl transformation
	urlch := make(chan *objurl.ObjectURL)
	go func() {
		defer close(urlch)

		for object := range objChan {
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
