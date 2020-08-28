package storage

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"os"
	"path/filepath"

	"github.com/karrick/godirwalk"
	"github.com/termie/go-shutil"

	"github.com/peak/s5cmd/storage/url"
)

// Filesystem is the Storage implementation of a local filesystem.
type Filesystem struct {
	dryRun bool
}

// FileReader is abstraction of file reading along with
// metadata about its content type.
type FileReader interface {
	io.ReadCloser
	ContentType() string
}

// FileWriter is abstraction of writing to local.
type FileWriter interface {
	io.WriteCloser
	io.WriterAt
}

// stdInOut implements FileReader and FileWriter interfaces.
// it is a wrapper for os.Stdin and os.Stdout so that they can be
// treated as a local file with security for operations.
type stdInOut struct {
	file *os.File
}

func (s *stdInOut) Write(p []byte) (n int, err error) {
	return s.file.Write(p)
}

func (s *stdInOut) WriteAt(p []byte, off int64) (n int, err error) {
	return s.file.WriteAt(p, off)
}

func (s *stdInOut) Read(p []byte) (n int, err error) {
	return s.file.Read(p)
}

func (s *stdInOut) Close() error {
	return nil
}

func (s *stdInOut) ContentType() string {
	// piped read from os.Stdin is a plain text.
	return "text/plain; charset=utf-8"
}

// localFile implements FileReader and FileWriter
// and wraps local file.
type localFile struct {
	*os.File
}

func (f *localFile) ContentType() string {
	contentType := mime.TypeByExtension(filepath.Ext(f.Name()))
	if contentType == "" {
		defer f.Seek(0, io.SeekStart)

		const bufsize = 512
		buf, err := ioutil.ReadAll(io.LimitReader(f, bufsize))
		if err != nil {
			return ""
		}

		return http.DetectContentType(buf)
	}
	return contentType
}

// Stat returns the Object structure describing object.
func (f *Filesystem) Stat(_ context.Context, url *url.URL) (*Object, error) {
	var stat os.FileInfo

	if url.IsStdinOut() {
		st, err := os.Stdin.Stat()
		if err != nil {
			return nil, err
		}
		stat = st
	} else {
		st, err := os.Stat(url.Absolute())
		if err != nil {
			if os.IsNotExist(err) {
				return nil, ErrGivenObjectNotFound
			}
			return nil, err
		}
		stat = st
	}

	mod := stat.ModTime()
	return &Object{
		URL:     url,
		Type:    ObjectType{stat.Mode()},
		Size:    stat.Size(),
		ModTime: &mod,
		Etag:    "",
	}, nil
}

// List returns the objects and directories reside in given src.
func (f *Filesystem) List(ctx context.Context, src *url.URL, followSymlinks bool) <-chan *Object {
	obj, err := f.Stat(ctx, src)
	isDir := err == nil && obj.Type.IsDir()

	if isDir {
		return f.walkDir(ctx, src, followSymlinks)
	}

	if src.HasGlob() {
		return f.expandGlob(ctx, src, followSymlinks)
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

			fileurl, _ := url.New(filename)
			fileurl.SetRelative(src.Absolute())

			obj, _ := f.Stat(ctx, fileurl)

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
	if !ShouldProcessUrl(src, followSymlinks) {
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

			fileurl.SetRelative(src.Absolute())

			//skip if symlink is pointing to a file and --no-follow-symlink
			if !ShouldProcessUrl(fileurl, followSymlinks) {
				return nil
			}

			obj, err := fs.Stat(ctx, fileurl)

			if err != nil {
				return err
			}
			fn(obj)
			return nil
		},
		// flags
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
func (f *Filesystem) Copy(_ context.Context, src, dst *url.URL, _ Metadata) error {
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
func (f *Filesystem) Delete(_ context.Context, url *url.URL) error {
	if f.dryRun || url.IsStdinOut() {
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

// Reader returns a FileReader for the given url.
func (f *Filesystem) Reader(u *url.URL) (FileReader, error) {
	if u.IsStdinOut() {
		return &stdInOut{file: os.Stdin}, nil
	}

	file, err := os.OpenFile(u.Absolute(), os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &localFile{file}, nil
}

// Writer returns a FileWriter for the given url.
func (f *Filesystem) Writer(u *url.URL) (FileWriter, error) {
	if u.IsStdinOut() {
		return &stdInOut{file: os.Stdout}, nil
	}
	if f.dryRun {
		return &os.File{}, nil
	}

	return os.Create(u.Absolute())
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
