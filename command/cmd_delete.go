package command

import (
	"context"
	"fmt"
	"sync"

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
	objChan, err := expandSources(ctx, srcurls...)
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

// expandSources is a non-blocking argument dispatcher.
// It creates a object channel by walking and expanding the given source urls.
// If the url has a glob, it creates a goroutine to list storage items and sends them to
// object channel, otherwise it creates storage object from the original source.
func expandSources(
	ctx context.Context,
	srcurls ...*objurl.ObjectURL,
) (<-chan *storage.Object, error) {
	// all sources share same client
	client, err := storage.NewClient(srcurls[0])
	if err != nil {
		return nil, err
	}

	objChan := make(chan *storage.Object)
	go func() {
		defer close(objChan)

		var wg sync.WaitGroup
		var objFound bool

		for _, origSrc := range srcurls {
			// call storage.List for only walking operations.
			if origSrc.HasGlob() {
				wg.Add(1)
				go func(origSrc *objurl.ObjectURL) {
					defer wg.Done()
					for object := range client.List(ctx, origSrc, true) {
						if object.Err == storage.ErrNoObjectFound {
							continue
						}
						objChan <- object
						objFound = true
					}
				}(origSrc)
			} else {
				objChan <- &storage.Object{URL: origSrc}
				objFound = true
			}
		}

		wg.Wait()
		if !objFound {
			objChan <- &storage.Object{Err: storage.ErrNoObjectFound}
		}
	}()

	return objChan, nil
}

// newSources creates ObjectURL list from given source strings.
func newSources(sources ...string) ([]*objurl.ObjectURL, error) {
	var urls []*objurl.ObjectURL
	for _, src := range sources {
		srcurl, err := objurl.New(src)
		if err != nil {
			return nil, err
		}
		urls = append(urls, srcurl)
	}
	return urls, nil
}

// checkSources check if given sources share same objurlType and gives
// error if it contains both local and remote targets.
func checkSources(sources ...string) error {
	var hasRemote, hasLocal bool
	for _, src := range sources {
		srcurl, err := objurl.New(src)
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
