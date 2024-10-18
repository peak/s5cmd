package storage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/karrick/godirwalk"
	"github.com/termie/go-shutil"

	"github.com/peak/s5cmd/v2/storage/url"
)

// Filesystem is the Storage implementation of a local filesystem.
type Filesystem struct {
	dryRun bool
}

// Stat returns the Object structure describing object.
func (f *Filesystem) Stat(ctx context.Context, url *url.URL) (*Object, error) {
	st, err := os.Stat(url.Absolute())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, &ErrGivenObjectNotFound{ObjectAbsPath: url.Absolute()}
		}
		return nil, err
	}

	mod := st.ModTime()
	return &Object{
		URL:     url,
		Type:    ObjectType{st.Mode()},
		Size:    st.Size(),
		ModTime: &mod,
		Etag:    "",
	}, nil
}

// List returns the objects and directories reside in given src.
func (f *Filesystem) List(ctx context.Context, src *url.URL, followSymlinks bool) <-chan *Object {
	if src.IsWildcard() {
		return f.expandGlob(ctx, src, followSymlinks)
	}

	obj, err := f.Stat(ctx, src)

	isDir := err == nil && obj.Type.IsDir()

	if isDir {
		return f.walkDir(ctx, src, followSymlinks)
	}

	return f.listSingleObject(ctx, src)
}

func (f *Filesystem) listSingleObject(ctx context.Context, src *url.URL) <-chan *Object {
	ch := make(chan *Object, 1)
	defer close(ch)

	object, err := f.Stat(ctx, src)
	if err != nil {
		object = &Object{Err: err}
	}
	ch <- object
	return ch
}

func (f *Filesystem) expandGlob(ctx context.Context, src *url.URL, followSymlinks bool) <-chan *Object {
	ch := make(chan *Object)

	go func() {
		defer close(ch)

		matchedFiles, err := filepath.Glob(src.Absolute())
		if err != nil {
			sendError(ctx, err, ch)
			return
		}
		if len(matchedFiles) == 0 {
			err := fmt.Errorf("no match found for %q", src)
			sendError(ctx, err, ch)
			return
		}

		for _, filename := range matchedFiles {
			filename := filename

			fileurl, err := url.New(filename)
			if err != nil {
				sendError(ctx, err, ch)
				return
			}

			fileurl.SetRelative(src)

			obj, err := f.Stat(ctx, fileurl)
			if err != nil {
				sendError(ctx, err, ch)
				return
			}

			if !obj.Type.IsDir() {
				sendObject(ctx, obj, ch)
				continue
			}

			walkDir(ctx, f, fileurl, followSymlinks, func(obj *Object) {
				sendObject(ctx, obj, ch)
			})
		}
	}()
	return ch
}

func walkDir(ctx context.Context, fs *Filesystem, src *url.URL, followSymlinks bool, fn func(o *Object)) {
	//skip if symlink is pointing to a dir and --no-follow-symlink
	if !ShouldProcessURL(src, followSymlinks) {
		return
	}
	err := godirwalk.Walk(src.Absolute(), &godirwalk.Options{
		Callback: func(pathname string, dirent *godirwalk.Dirent) error {
			// we're interested in files
			if dirent.IsDir() {
				return nil
			}

			fileurl, err := url.New(pathname)
			if err != nil {
				return err
			}

			fileurl.SetRelative(src)

			//skip if symlink is pointing to a file and --no-follow-symlink
			if !ShouldProcessURL(fileurl, followSymlinks) {
				return nil
			}

			obj, err := fs.Stat(ctx, fileurl)
			if err != nil {
				return err
			}

			fn(obj)
			return nil
		},
		FollowSymbolicLinks: followSymlinks,
	})
	if err != nil {
		obj := &Object{Err: err}
		fn(obj)
	}
}

func (f *Filesystem) walkDir(ctx context.Context, src *url.URL, followSymlinks bool) <-chan *Object {
	ch := make(chan *Object)
	go func() {
		defer close(ch)

		walkDir(ctx, f, src, followSymlinks, func(obj *Object) {
			sendObject(ctx, obj, ch)
		})
	}()
	return ch
}

// Copy copies given source to destination.
func (f *Filesystem) Copy(ctx context.Context, src, dst *url.URL, _ Metadata) error {
	if f.dryRun {
		return nil
	}

	if err := os.MkdirAll(dst.Dir(), os.ModePerm); err != nil {
		return err
	}
	_, err := shutil.Copy(src.Absolute(), dst.Absolute(), true)
	return err
}

// Delete deletes given file.
func (f *Filesystem) Delete(ctx context.Context, url *url.URL) error {
	if f.dryRun {
		return nil
	}

	return os.Remove(url.Absolute())
}

// MultiDelete deletes all files returned from given channel.
func (f *Filesystem) MultiDelete(ctx context.Context, urlch <-chan *url.URL) <-chan *Object {
	resultch := make(chan *Object)
	go func() {
		defer close(resultch)

		for url := range urlch {
			err := f.Delete(ctx, url)
			obj := &Object{
				URL: url,
				Err: err,
			}
			resultch <- obj
		}
	}()
	return resultch
}

// MkdirAll calls os.MkdirAll.
func (f *Filesystem) MkdirAll(path string) error {
	if f.dryRun {
		return nil
	}
	return os.MkdirAll(path, os.ModePerm)
}

// Create creates a new os.File.
func (f *Filesystem) Create(path string) (*os.File, error) {
	if f.dryRun {
		return &os.File{}, nil
	}

	return os.Create(path)
}

// Open opens the given source.
func (f *Filesystem) Open(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// CreateTemp creates a new temporary file
func (f *Filesystem) CreateTemp(dir, pattern string) (*os.File, error) {
	if f.dryRun {
		return &os.File{}, nil
	}

	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}

	err = file.Chmod(0644)
	return file, err
}

// Rename a file
func (f *Filesystem) Rename(file *os.File, newpath string) error {
	if f.dryRun {
		return nil
	}

	return os.Rename(file.Name(), newpath)
}

func sendObject(ctx context.Context, obj *Object, ch chan *Object) {
	select {
	case <-ctx.Done():
	case ch <- obj:
	}
}

func sendError(ctx context.Context, err error, ch chan *Object) {
	obj := &Object{Err: err}
	sendObject(ctx, obj, ch)
}
