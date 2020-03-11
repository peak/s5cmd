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
	HelpName: "delete",
	Usage:    "TODO",
	Before: func(c *cli.Context) error {
		validate := func() error {
			if !c.Args().Present() {
				return fmt.Errorf("expected at least 1 object to remove")
			}
			return nil
		}
		if err := validate(); err != nil {
			printError(givenCommand(c), c.Command.Name, err)
			return err
		}
		return nil
	},
	Action: func(c *cli.Context) error {
		err := Delete(
			c.Context,
			c.Command.Name,
			givenCommand(c),
			c.Args().Slice()...,
		)
		if err != nil {
			printError(givenCommand(c), c.Command.Name, err)
			return err
		}

		return nil
	},
}

func Delete(
	ctx context.Context,
	op string,
	fullCommand string,
	args ...string,
) error {
	sources := make([]*objurl.ObjectURL, len(args))
	for i, arg := range args {
		url, _ := objurl.New(arg)
		sources[i] = url
	}

	client, err := storage.NewClient(sources[0])
	if err != nil {
		return err
	}

	// do object->objurl transformation
	urlch := make(chan *objurl.ObjectURL)

	go func() {
		defer close(urlch)

		// there are multiple source files which are received from batch-rm
		// command.
		if len(sources) > 1 {
			for _, url := range sources {
				select {
				case <-ctx.Done():
					return
				case urlch <- url:
				}
			}
		} else {
			// src is a glob
			src := sources[0]
			for object := range client.List(ctx, src, true, storage.ListAllItems) {
				if object.Type.IsDir() || errorpkg.IsCancelation(object.Err) {
					continue
				}

				if err := object.Err; err != nil {
					printError(fullCommand, op, err)
					continue
				}
				urlch <- object.URL
			}
		}
	}()

	resultch := client.MultiDelete(ctx, urlch)

	// closed errch indicates that MultiDelete operation is finished.
	var merror error
	for obj := range resultch {
		if err := obj.Err; err != nil {
			if errorpkg.IsCancelation(obj.Err) {
				continue
			}

			merror = multierror.Append(merror, obj.Err)
			printError(fullCommand, op, err)
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
