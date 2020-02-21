package storage

import (
	"context"
	"fmt"
	"io"
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
			obj := &Object{Err: err}
			sendObject(ctx, obj, ch)
			return
		}
		if len(matchedFiles) == 0 {
			obj := &Object{Err: fmt.Errorf("no match found for %q", url)}
			sendObject(ctx, obj, ch)
			return
		}

		for _, filename := range matchedFiles {
			filename := filename
			url, _ := objurl.New(filename)
			obj, _ := f.Stat(ctx, url)

			if !obj.Type.IsDir() {
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
						Type: dirent.ModeType(),
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

func sendObject(ctx context.Context, obj *Object, ch chan *Object) {
	select {
	case <-ctx.Done():
	case ch <- obj:
	}
}

func (f *Filesystem) walkDir(ctx context.Context, url *objurl.ObjectURL, isRecursive bool) <-chan *Object {
	ch := make(chan *Object)
	go func() {
		defer close(ch)

		// there's no use case for a Readdir call in the codebase.
		if !isRecursive {
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
					Type: dirent.ModeType(),
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
	return f.stats
}

func (f *Filesystem) notimplemented(method string) error {
	return notImplemented{
		apiType: "filesystem",
		method:  method,
	}
}
