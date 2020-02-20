package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/karrick/godirwalk"
	"github.com/termie/go-shutil"

	"github.com/peak/s5cmd/objurl"
)

type Filesystem struct {
}

func NewFilesystem() *Filesystem { return &Filesystem{} }

func (f *Filesystem) Stat(ctx context.Context, url *objurl.ObjectURL) (*Object, error) {
	st, err := os.Stat(url.Absolute())
	if err != nil {
		return nil, err
	}

	return &Object{
		URL:     url,
		Type:    st.Mode(),
		Size:    st.Size(),
		ModTime: st.ModTime(),
		Etag:    "",
	}, nil
}

func (f *Filesystem) List(ctx context.Context, url *objurl.ObjectURL, isRecursive bool, _ int64) <-chan *Object {
	obj, err := f.Stat(ctx, url)
	isDir := err == nil && obj.Type.IsDir()

	if isDir {
		if isRecursive {
			return f.walkDir(ctx, url)
		}

		ch := make(chan *Object)
		close(ch)
		return ch
	}

	return f.expandGlob(ctx, url)
}

func (f *Filesystem) expandGlob(ctx context.Context, url *objurl.ObjectURL) <-chan *Object {
	ch := make(chan *Object)
	go func() {
		defer close(ch)

		matchedFiles, err := filepath.Glob(url.Absolute())
		if err != nil {
			// TODO(ig): expose error
			return
		}
		if len(matchedFiles) == 0 {
			// TODO(ig): expose "no match found" error
			return
		}

		for _, filename := range matchedFiles {
			url, _ := objurl.New(filename)
			obj, _ := f.Stat(ctx, url)

			ch <- obj
		}
	}()
	return ch
}

func (f *Filesystem) walkDir(ctx context.Context, url *objurl.ObjectURL) <-chan *Object {
	ch := make(chan *Object)
	go func() {
		defer close(ch)

		godirwalk.Walk(url.Absolute(), &godirwalk.Options{
			Callback: func(pathname string, dirent *godirwalk.Dirent) error {
				url, err := objurl.New(pathname)
				if err != nil {
					return err
				}

				obj := &Object{
					URL:  url,
					Type: dirent.ModeType(),
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case ch <- obj:
				}

				return nil
			},
			// TODO(ig): enable following symlink once we have the necessary cli
			// flags
			FollowSymbolicLinks: false,
		})
	}()
	return ch
}

func (f *Filesystem) Copy(ctx context.Context, src, dst *objurl.ObjectURL, _ string) error {
	_, err := shutil.Copy(src.Absolute(), dst.Absolute(), true)
	return err
}

func (f *Filesystem) Delete(ctx context.Context, urls ...*objurl.ObjectURL) error {
	// TODO(ig): use multierr or a chan error
	var rerr error
	for _, url := range urls {
		err := os.Remove(url.Absolute())
		if err != nil {
			rerr = err
		}
	}
	return rerr
}

func (f *Filesystem) Put(ctx context.Context, body io.Reader, url *objurl.ObjectURL, _ string) error {
	return f.notimplemented("Put")
}

func (f *Filesystem) Get(_ context.Context, _ *objurl.ObjectURL, _ io.WriterAt) error {
	return f.notimplemented("Get")
}

func (f *Filesystem) ListBuckets(_ context.Context, _ string) ([]Bucket, error) {
	return nil, f.notimplemented("ListBuckets")
}

func (f *Filesystem) UpdateRegion(_ string) error {
	return f.notimplemented("UpdateRegion")
}

func (f *Filesystem) Statistics() *Stats {
	panic("not implemented") // TODO: Implement
}

func (f *Filesystem) notimplemented(method string) error {
	return notImplemented{
		apiType: "filesystem",
		method:  method,
	}
}
