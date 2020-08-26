package command

import (
	"context"

	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
)

// expandSource returns the full list of objects from the given src argument.
// If src is an expandable URL, such as directory, prefix or a glob, all
// objects are returned by walking the source.
func expandSource(
	ctx context.Context,
	followSymlinks bool,
	srcurl *url.URL,
	storageOpts storage.Options,
) (<-chan *storage.Object, error) {
	client, err := storage.NewClient(srcurl, storageOpts)
	if err != nil {
		return nil, err
	}

	var isDir bool
	// if the source is local, we send a Stat call to know if  we have
	// directory or file to walk. For remote storage, we don't want to send
	// Stat since it doesn't have any folder semantics.
	if !srcurl.HasGlob() && !srcurl.IsRemote() {
		obj, err := client.Stat(ctx, srcurl)
		if err != nil {
			return nil, err
		}
		isDir = obj.Type.IsDir()
	}

	// call storage.List for only walking operations.
	if srcurl.HasGlob() || isDir {
		return client.List(ctx, srcurl, followSymlinks), nil
	}

	ch := make(chan *storage.Object, 1)
	if storage.ShouldProcessUrl(srcurl, followSymlinks) {
		ch <- &storage.Object{URL: srcurl}
	}
	close(ch)
	return ch, nil
}
