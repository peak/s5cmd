package storage

import (
	"context"
	"io"

	"github.com/karrick/godirwalk"

	"github.com/peak/s5cmd/objurl"
)

type Filesystem struct {
}

func NewFilesystem() *Filesystem { return &Filesystem{} }

func (f *Filesystem) Stat(ctx context.Context, url *objurl.ObjectURL) (*Object, error) {
	panic("not implemented") // TODO: Implement
}

func (f *Filesystem) List(ctx context.Context, url *objurl.ObjectURL, _ int64) <-chan *Object {
	ch := make(chan *Object)

	godirwalk.Walk(url.Absolute(), &godirwalk.Options{
		Callback: func(pathname string, dirent *godirwalk.Dirent) error {
			url, err := objurl.New(pathname)
			if err != nil {
				return err
			}

			obj := &Object{
				URL:         url,
				IsDirectory: dirent.IsDir(),
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
	return ch
}

func (f *Filesystem) Copy(ctx context.Context, from, to *objurl.ObjectURL, _ string) error {
	panic("not implemented") // TODO: Implement
}

func (f *Filesystem) Get(_ context.Context, _ *objurl.ObjectURL, _ io.WriterAt) error {
	return f.notimplemented("Get")
}

func (f *Filesystem) Put(ctx context.Context, body io.Reader, url *objurl.ObjectURL, _ string) error {
	return f.notimplemented("Put")
}

func (f *Filesystem) Delete(ctx context.Context, _ string, _ ...*objurl.ObjectURL) error {
	panic("not implemented") // TODO: Implement
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
