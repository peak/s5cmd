package command

import (
	"context"
	"fmt"
	"sync"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/storage"
)

// expandSources returns the full list of objects from the given src arguments.
// If src is an expandable URL, such as directory, prefix or a glob, all
// objects are returned by walking the source. It expands multiple resources asynchronously
// and returns read-only arg channel.
func expandSources(
	ctx context.Context,
	isRecursive bool,
	dsturl *objurl.ObjectURL,
	srcurls ...*objurl.ObjectURL,
) (<-chan *storage.Object, error) {
	if len(srcurls) == 0 {
		return nil, fmt.Errorf("at least one source url is required")
	}
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
			var isDir bool
			// if the source is local, we send a Stat call to know if  we have
			// directory or file to walk. For remote storage, we don't want to send
			// Stat since it doesn't have any folder semantics.
			if !origSrc.HasGlob() && !origSrc.IsRemote() {
				obj, err := client.Stat(ctx, origSrc)
				if err != nil {
					if err != storage.ErrGivenObjectNotFound {
						objChan <- &storage.Object{Err: err}
					}
					continue
				}
				isDir = obj.Type.IsDir()
			}

			recursive := isRecursive
			if !recursive && dsturl != nil {
				// set recursive=true for local->remote copy operations. this
				// is required for backwards compatibility.
				recursive = !origSrc.IsRemote() && dsturl.IsRemote()
			}

			// call storage.List for only walking operations.
			if origSrc.HasGlob() || isDir {
				wg.Add(1)
				go func(origSrc *objurl.ObjectURL) {
					defer wg.Done()
					for object := range client.List(ctx, origSrc, recursive, storage.ListAllItems) {
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
