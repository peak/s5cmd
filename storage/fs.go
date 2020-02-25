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

type Filesystem struct {
	stats *Stats
}

func NewFilesystem() *Filesystem {
	return &Filesystem{stats: &Stats{}}
}

func (f *Filesystem) Stat(ctx context.Context, url *objurl.ObjectURL) (*Object, error) {
	st, err := os.Stat(url.Absolute())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrGivenObjectNotFound
		}
		return nil, err
	}

	return &Object{
		URL:     url,
		Mode:    st.Mode(),
		Size:    st.Size(),
		ModTime: st.ModTime(),
		Etag:    "",
	}, nil
}

func (f *Filesystem) List(ctx context.Context, url *objurl.ObjectURL, isRecursive bool, _ int64) <-chan *Object {
	obj, err := f.Stat(ctx, url)
	isDir := err == nil && obj.Mode.IsDir()

	if isDir {
		return f.walkDir(ctx, url, isRecursive)
	}

	hasGlob := objurl.HasGlobCharacter(url.Absolute())
	if hasGlob {
		return f.expandGlob(ctx, url, isRecursive)
	}

	panic(fmt.Sprintf("unexpected visit for %q", url.Absolute()))
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
			url, _ := objurl.New(filename)
			obj, _ := f.Stat(ctx, url)

			if !obj.Mode.IsDir() {
				sendObject(ctx, obj, ch)
			}

			// don't walk the directory if not asked
			if !isRecursive {
				continue
			}

			godirwalk.Walk(url.Absolute(), &godirwalk.Options{
				Callback: func(pathname string, dirent *godirwalk.Dirent) error {
					// we're interested in files
					if dirent.IsDir() {
						return nil
					}

					url, err := objurl.New(pathname)
					if err != nil {
						return err
					}

					obj := &Object{
						URL:  url,
						Mode: dirent.ModeType(),
					}

					sendObject(ctx, obj, ch)
					return nil
				},
				// TODO(ig): enable following symlink once we have the necessary cli
				// flags
				FollowSymbolicLinks: false,
			})
		}
	}()
	return ch
}

func (f *Filesystem) readDir(ctx context.Context, url *objurl.ObjectURL, ch chan *Object) {
	dir := url.Absolute()
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		sendError(ctx, err, ch)
		return
	}

	for _, fi := range fis {
		filename := filepath.Join(dir, fi.Name())
		url, _ := objurl.New(filename)
		obj := &Object{
			URL:     url,
			ModTime: fi.ModTime(),
			Mode:    fi.Mode(),
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

		godirwalk.Walk(url.Absolute(), &godirwalk.Options{
			Callback: func(pathname string, dirent *godirwalk.Dirent) error {
				// we're interested in files
				if dirent.IsDir() {
					return nil
				}

				url, err := objurl.New(pathname)
				if err != nil {
					return err
				}

				obj := &Object{
					URL:  url,
					Mode: dirent.ModeType(),
				}

				sendObject(ctx, obj, ch)

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
	for _, url := range urls {
		fpath := url.Absolute()
		err := os.Remove(fpath)
		if err != nil {
			f.stats.put(fpath, StatsResponse{
				Success: false,
				Message: err.Error(),
			})
		} else {
			f.stats.put(fpath, StatsResponse{Success: true})
		}
	}
	return nil
}

func (f *Filesystem) Put(ctx context.Context, body io.Reader, url *objurl.ObjectURL, _ map[string]string) error {
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
	return f.stats
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
