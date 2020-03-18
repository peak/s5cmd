package command

import (
	"context"
	"fmt"
	"sync"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/storage"
)

// Arg is a container type for supporting variadic arguments.
type Arg struct {
	origSrc *objurl.ObjectURL
	obj     *storage.Object
}

// expandSources returns the full list of objects from the given src arguments.
// If src is an expandable URL, such as directory, prefix or a glob, all
// objects are returned by walking the source. It expands multiple resources asynchronously
// and returns read-only arg channel.
func expandSources(
	ctx context.Context,
	isRecursive bool,
	dst *objurl.ObjectURL,
	sources ...*objurl.ObjectURL,
) (<-chan *Arg, error) {
	// all sources share same client
	srcurl := sources[0]
	client, err := storage.NewClient(srcurl)
	if err != nil {
		return nil, err
	}

	argChan := make(chan *Arg)
	go func() {
		defer close(argChan)

		var wg sync.WaitGroup
		var objFound bool

		for _, src := range sources {
			var isDir bool
			// if the source is local, we send a Stat call to know if  we have
			// directory or file to walk. For remote storage, we don't want to send
			// Stat since it doesn't have any folder semantics.
			if !src.HasGlob() && !src.IsRemote() {
				obj, err := client.Stat(ctx, src)
				if err != nil {
					if err != storage.ErrGivenObjectNotFound {
						argChan <- &Arg{
							origSrc: src,
							obj:     &storage.Object{Err: err},
						}
					}
					continue
				}
				isDir = obj.Type.IsDir()
			}

			recursive := isRecursive
			if !recursive && dst != nil {
				// set recursive=true for local->remote copy operations. this
				// is required for backwards compatibility.
				recursive = !src.IsRemote() && dst.IsRemote()
			}

			// call storage.List for only walking operations.
			if src.HasGlob() || isDir {
				wg.Add(1)
				go func(originalUrl *objurl.ObjectURL) {
					defer wg.Done()
					for obj := range client.List(ctx, originalUrl, recursive, storage.ListAllItems) {
						if obj.Err == storage.ErrNoObjectFound {
							continue
						}
						argChan <- &Arg{
							origSrc: originalUrl,
							obj:     obj,
						}
						objFound = true
					}
				}(src)
			} else {
				argChan <- &Arg{
					origSrc: src,
					obj:     &storage.Object{URL: src},
				}
				objFound = true
			}
		}

		wg.Wait()
		if !objFound {
			argChan <- &Arg{obj: &storage.Object{Err: storage.ErrNoObjectFound}}
		}
	}()

	return argChan, nil
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
