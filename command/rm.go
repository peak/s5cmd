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

var deleteHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} argument [argument]

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Delete an S3 object
		 > s5cmd {{.HelpName}} s3://bucketname/prefix/object.gz

	2. Delete all objects with a prefix
		 > s5cmd {{.HelpName}} s3://bucketname/prefix/*

	3. Delete all objects that matches a wildcard
		 > s5cmd {{.HelpName}} s3://bucketname/*/obj*.gz

	4. Delete all matching objects and a specific object
		 > s5cmd {{.HelpName}} s3://bucketname/prefix/* s3://bucketname/object1.gz
`

var deleteCommand = &cli.Command{
	Name:               "rm",
	HelpName:           "rm",
	Usage:              "remove objects",
	CustomHelpTemplate: deleteHelpTemplate,
	Before: func(c *cli.Context) error {
		if !c.Args().Present() {
			return fmt.Errorf("expected at least 1 object to remove")
		}

		return sourcesHaveSameType(c.Args().Slice()...)
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

// Delete remove given sources.
func Delete(
	ctx context.Context,
	op string,
	fullCommand string,
	src ...string,
) error {
	srcurls, err := newURLs(src...)
	if err != nil {
		return err
	}
	srcurl := srcurls[0]

	client, err := storage.NewClient(srcurl, AppStorageOptions)
	if err != nil {
		return err
	}

	objChan := expandSources(ctx, client, false, srcurls...)

	// do object->url transformation
	urlch := make(chan *url.URL)
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

// newSources creates object URL list from given sources.
func newURLs(sources ...string) ([]*url.URL, error) {
	var urls []*url.URL
	for _, src := range sources {
		srcurl, err := url.New(src)
		if err != nil {
			return nil, err
		}
		urls = append(urls, srcurl)
	}
	return urls, nil
}

// sourcesHaveSameType check if given sources share the same object types.
func sourcesHaveSameType(sources ...string) error {
	var hasRemote, hasLocal bool
	for _, src := range sources {
		srcurl, err := url.New(src)
		if err != nil {
			return err
		}

		// we don't operate on S3 prefixes for copy and delete operations.
		if srcurl.IsBucket() || srcurl.IsPrefix() {
			return fmt.Errorf("source argument must contain wildcard character")
		}

		if srcurl.IsRemote() {
			hasRemote = true
		} else {
			hasLocal = true
		}

		if hasLocal && hasRemote {
			return fmt.Errorf("arguments cannot have both local and remote sources")
		}
	}
	return nil
}
