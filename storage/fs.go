package storage

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/karrick/godirwalk"
	"github.com/termie/go-shutil"

	"github.com/peak/s5cmd/storage/url"
)

type Filesystem struct{}

func NewFilesystem() *Filesystem {
	return &Filesystem{}
}

func (f *Filesystem) Stat(ctx context.Context, url *url.URL) (*Object, error) {
	st, err := os.Stat(url.Absolute())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrGivenObjectNotFound
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

func (f *Filesystem) List(ctx context.Context, src *url.URL) <-chan *Object {
	obj, err := f.Stat(ctx, src)
	isDir := err == nil && obj.Type.IsDir()

	if isDir {
		return f.walkDir(ctx, src)
	}

	if src.HasGlob() {
		return f.expandGlob(ctx, src)
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

func (f *Filesystem) expandGlob(ctx context.Context, src *url.URL) <-chan *Object {
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

			fileurl, _ := url.New(filename)
			fileurl.SetRelative(src.Absolute())

			obj, _ := f.Stat(ctx, fileurl)

			if !obj.Type.IsDir() {
				sendObject(ctx, obj, ch)
				continue
			}

			walkDir(ctx, f, fileurl, func(obj *Object) {
				sendObject(ctx, obj, ch)
			})
		}
	}()
	return ch
}

func walkDir(ctx context.Context, storage Storage, src *url.URL, fn func(o *Object)) {
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

			fileurl.SetRelative(src.Absolute())

			obj, err := storage.Stat(ctx, fileurl)
			if err != nil {
				return err
			}
			fn(obj)
			return nil
		},
		// TODO(ig): enable following symlink once we have the necessary cli
		// flags
		FollowSymbolicLinks: false,
	})
	if err != nil {
		obj := &Object{Err: err}
		fn(obj)
	}
}

func (f *Filesystem) readDir(ctx context.Context, src *url.URL, ch chan *Object) {
	dir := src.Absolute()
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		sendError(ctx, err, ch)
		return
	}

	for _, fi := range fis {
		mod := fi.ModTime()
		obj := &Object{
			URL:     src.Join(fi.Name()),
			ModTime: &mod,
			Type:    ObjectType{fi.Mode()},
			Size:    fi.Size(),
		}
		sendObject(ctx, obj, ch)
	}
}

func (f *Filesystem) walkDir(ctx context.Context, src *url.URL) <-chan *Object {
	ch := make(chan *Object)
	go func() {
		defer close(ch)

		walkDir(ctx, f, src, func(obj *Object) {
			sendObject(ctx, obj, ch)
		})
	}()
	return ch
}
func (f *Filesystem) Copy(ctx context.Context, src, dst *url.URL, _ map[string]string) error {
	if err := os.MkdirAll(dst.Dir(), os.ModePerm); err != nil {
		return err
	}
	_, err := shutil.Copy(src.Absolute(), dst.Absolute(), true)
	return err
}

func (f *Filesystem) Delete(ctx context.Context, url *url.URL) error {
	return os.Remove(url.Absolute())
}

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

func (f *Filesystem) Put(_ context.Context, _ io.Reader, _ *url.URL, _ map[string]string, _ int, _ int64) error {
	return f.notimplemented("Put")
}

func (f *Filesystem) Get(_ context.Context, _ *url.URL, _ io.WriterAt, _ int, _ int64) (int64, error) {
	return 0, f.notimplemented("Get")
}

func (f *Filesystem) ListBuckets(_ context.Context, _ string) ([]Bucket, error) {
	return nil, f.notimplemented("ListBuckets")
}

func (f *Filesystem) MakeBucket(_ context.Context, _ string) error {
	return f.notimplemented("MakeBucket")
}

func (f *Filesystem) notimplemented(method string) error {
	return notImplemented{
		apiType: "filesystem",
		method:  method,
	}
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
