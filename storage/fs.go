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

	"github.com/peak/s5cmd/objurl"
)

type Filesystem struct{}

func NewFilesystem() *Filesystem {
	return &Filesystem{}
}

func (f *Filesystem) Stat(ctx context.Context, url *objurl.ObjectURL) (*Object, error) {
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

func (f *Filesystem) List(ctx context.Context, url *objurl.ObjectURL, isRecursive bool) <-chan *Object {
	obj, err := f.Stat(ctx, url)
	isDir := err == nil && obj.Type.IsDir()

	if isDir {
		return f.walkDir(ctx, url, isRecursive)
	}

	if url.HasGlob() {
		return f.expandGlob(ctx, url, isRecursive)
	}

	return f.listSingleObject(ctx, url)
}

func (f *Filesystem) listSingleObject(ctx context.Context, url *objurl.ObjectURL) <-chan *Object {
	ch := make(chan *Object, 1)
	defer close(ch)

	object, err := f.Stat(ctx, url)
	if err != nil {
		object = &Object{Err: err}
	}
	ch <- object
	return ch
}

func (f *Filesystem) expandGlob(ctx context.Context, url *objurl.ObjectURL, isRecursive bool) <-chan *Object {
	ch := make(chan *Object)

	go func() {
		defer close(ch)

		matchedFiles, err := filepath.Glob(url.Absolute())
		if err != nil {
			sendError(ctx, err, ch)
			return
		}
		if len(matchedFiles) == 0 {
			err := fmt.Errorf("no match found for %q", url)
			sendError(ctx, err, ch)
			return
		}

		for _, filename := range matchedFiles {
			filename := filename

			fileurl, _ := objurl.New(filename)
			fileurl.SetRelative(url.Absolute())

			obj, _ := f.Stat(ctx, fileurl)

			if !obj.Type.IsDir() {
				sendObject(ctx, obj, ch)
				continue
			}

			// don't walk the directory if not asked
			if !isRecursive {
				continue
			}

			walkDir(ctx, f, fileurl, func(obj *Object) {
				sendObject(ctx, obj, ch)
			})
		}
	}()
	return ch
}

func walkDir(ctx context.Context, storage Storage, url *objurl.ObjectURL, fn func(o *Object)) {
	err := godirwalk.Walk(url.Absolute(), &godirwalk.Options{
		Callback: func(pathname string, dirent *godirwalk.Dirent) error {
			// we're interested in files
			if dirent.IsDir() {
				return nil
			}

			fileurl, err := objurl.New(pathname)
			if err != nil {
				return err
			}

			fileurl.SetRelative(url.Absolute())

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

func (f *Filesystem) readDir(ctx context.Context, url *objurl.ObjectURL, ch chan *Object) {
	dir := url.Absolute()
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		sendError(ctx, err, ch)
		return
	}

	for _, fi := range fis {
		mod := fi.ModTime()
		obj := &Object{
			URL:     url.Join(fi.Name()),
			ModTime: &mod,
			Type:    ObjectType{fi.Mode()},
			Size:    fi.Size(),
		}
		sendObject(ctx, obj, ch)
	}
}

func (f *Filesystem) walkDir(ctx context.Context, url *objurl.ObjectURL, isRecursive bool) <-chan *Object {
	ch := make(chan *Object)
	go func() {
		defer close(ch)

		if !isRecursive {
			f.readDir(ctx, url, ch)
			return
		}

		walkDir(ctx, f, url, func(obj *Object) {
			sendObject(ctx, obj, ch)
		})
	}()
	return ch
}
func (f *Filesystem) Copy(ctx context.Context, src, dst *objurl.ObjectURL, _ map[string]string) error {
	if err := os.MkdirAll(dst.Dir(), os.ModePerm); err != nil {
		return err
	}
	_, err := shutil.Copy(src.Absolute(), dst.Absolute(), true)
	return err
}

func (f *Filesystem) Delete(ctx context.Context, url *objurl.ObjectURL) error {
	return os.Remove(url.Absolute())
}

func (f *Filesystem) MultiDelete(ctx context.Context, urlch <-chan *objurl.ObjectURL) <-chan *Object {
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

func (f *Filesystem) Put(ctx context.Context, body io.Reader, url *objurl.ObjectURL, _ map[string]string) error {
	return f.notimplemented("Put")
}

func (f *Filesystem) Get(_ context.Context, _ *objurl.ObjectURL, _ io.WriterAt) (int64, error) {
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
