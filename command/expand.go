package command

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
)

// expandSource returns the full list of objects from the given src argument.
// If src is an expandable URL, such as directory, prefix or a glob, all
// objects are returned by walking the source.
func expandSource(
	ctx context.Context,
	client storage.Storage,
	followSymlinks bool,
	srcurl *url.URL,
) (<-chan *storage.Object, error) {
	var objType storage.ObjectType
	// if the source is local, we send a Stat call to know if  we have
	// directory or file to walk. For remote storage, we don't want to send
	// Stat since it doesn't have any folder semantics.
	if !srcurl.IsWildcard() && !srcurl.IsRemote() {
		obj, err := client.Stat(ctx, srcurl)
		if err != nil {
			return nil, err
		}
		objType = obj.Type
	}

	// call storage.List for only walking operations.
	if srcurl.IsWildcard() || srcurl.AllVersions || objType.IsDir() {
		return client.List(ctx, srcurl, followSymlinks), nil
	}

	ch := make(chan *storage.Object, 1)
	if storage.ShouldProcessURL(srcurl, followSymlinks) {
		ch <- &storage.Object{URL: srcurl, Type: objType}
	}
	close(ch)
	return ch, nil
}

// expandSources is a non-blocking argument dispatcher. It creates a object
// channel by walking and expanding the given source urls. If the url has a
// glob, it creates a goroutine to list storage items and sends them to object
// channel, otherwise it creates storage object from the original source.
func expandSources(
	ctx context.Context,
	client storage.Storage,
	followSymlinks bool,
	srcurls ...*url.URL,
) <-chan *storage.Object {
	ch := make(chan *storage.Object)

	go func() {
		defer close(ch)

		var wg sync.WaitGroup
		var objFound atomic.Bool

		for _, origSrc := range srcurls {
			wg.Add(1)
			go func(origSrc *url.URL) {
				defer wg.Done()

				objch, err := expandSource(ctx, client, followSymlinks, origSrc)
				if err != nil {
					var objNotFound *storage.ErrGivenObjectNotFound
					if !errors.As(err, &objNotFound) {
						ch <- &storage.Object{Err: err}
					}
					return
				}

				for object := range objch {
					if object.Err == storage.ErrNoObjectFound {
						continue
					}
					ch <- object
					objFound.Store(true)
				}
			}(origSrc)
		}

		wg.Wait()
		if !objFound.Load() {
			ch <- &storage.Object{Err: storage.ErrNoObjectFound}
		}
	}()

	return ch
}
