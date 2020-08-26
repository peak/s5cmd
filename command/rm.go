package command

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/urfave/cli/v2"

	errorpkg "github.com/peak/s5cmd/error"
	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/log/stat"
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
		err := validateRMCommand(c)
		if err != nil {
			printError(givenCommand(c), c.Command.Name, err)
		}
		return err
	},
	Action: func(c *cli.Context) (err error) {
		defer stat.Collect(c.Command.FullName(), &err)()
		return Delete{
			src:         c.Args().Slice(),
			op:          c.Command.Name,
			fullCommand: givenCommand(c),
			storageOpts: NewStorageOpts(c),
		}.Run(c.Context)
	},
}

// Delete holds delete operation flags and states.
type Delete struct {
	src         []string
	op          string
	fullCommand string

	// storage options
	storageOpts storage.Options
}

// Run remove given sources.
func (d Delete) Run(ctx context.Context) error {
	srcurls, err := newURLs(d.src...)
	if err != nil {
		printError(d.fullCommand, d.op, err)
		return err
	}

	resultch := make(chan *storage.Object)

	var wg sync.WaitGroup
	go func() {
		defer close(resultch)

		for _, srcurl := range srcurls {
			wg.Add(1)
			go doDelete(ctx, srcurl, d.op, d.fullCommand, resultch, &wg, d.storageOpts)
			wg.Wait()
		}
	}()

	var merror error
	for obj := range resultch {
		if err := obj.Err; err != nil {
			if errorpkg.IsCancelation(obj.Err) {
				continue
			}

			merror = multierror.Append(merror, obj.Err)
			printError(d.fullCommand, d.op, obj.Err)
			continue
		}

		msg := log.InfoMessage{
			Operation: d.op,
			Source:    obj.URL,
		}
		log.Info(msg)
	}

	return merror
}

func doDelete(
	ctx context.Context,
	src *url.URL,
	op string,
	fullCommand string,
	ch chan<- *storage.Object,
	wg *sync.WaitGroup,
	storageOpts storage.Options,
) {
	defer wg.Done()

	objChan, err := expandSource(ctx, false, src, storageOpts)
	if err != nil {
		ch <- &storage.Object{Err: err}
		return
	}

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

	client, err := storage.NewClient(src, storageOpts)
	if err != nil {
		ch <- &storage.Object{Err: err}
		return
	}

	resultch := client.MultiDelete(ctx, urlch)
	for obj := range resultch {
		ch <- obj
	}
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

func validateRMCommand(c *cli.Context) error {
	if !c.Args().Present() {
		return fmt.Errorf("expected at least 1 object to remove")
	}

	return sourcesHaveSameType(c.Args().Slice()...)
}
