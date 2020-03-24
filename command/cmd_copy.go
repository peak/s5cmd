package command

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/hashicorp/go-multierror"
	"github.com/urfave/cli/v2"

	errorpkg "github.com/peak/s5cmd/error"
	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/parallel"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
)

var copyCommandFlags = []cli.Flag{
	&cli.BoolFlag{
		Name:    "no-clobber",
		Aliases: []string{"n"},
		Usage:   "do not overwrite destination if already exists",
	},
	&cli.BoolFlag{
		Name:    "if-size-differ",
		Aliases: []string{"s"},
		Usage:   "only overwrite destination if size differs",
	},
	&cli.BoolFlag{
		Name:    "if-source-newer",
		Aliases: []string{"u"},
		Usage:   "only overwrite destination if source modtime is newer",
	},
	&cli.BoolFlag{
		Name:  "parents",
		Usage: "create same directory structure of source, starting from the first wildcard",
	},
	&cli.BoolFlag{
		Name:    "recursive",
		Aliases: []string{"R"},
		Usage:   "command is performed on all objects under the given source",
	},
	&cli.StringFlag{
		Name:  "storage-class",
		Usage: "set storage class for target ('STANDARD','REDUCED_REDUNDANCY','GLACIER','STANDARD_IA')",
	},
	&cli.IntFlag{
		Name:    "concurrency",
		Aliases: []string{"c"},
		Value:   defaultCopyConcurrency,
		Usage:   "number of concurrent parts transferred between host and remote server",
	},
	&cli.IntFlag{
		Name:    "part-size",
		Aliases: []string{"p"},
		Value:   defaultPartSize,
		Usage:   "size of each part transferred between host and remote server, in MiB",
	},
}

var CopyCommand = &cli.Command{
	Name:     "cp",
	HelpName: "cp",
	Usage:    "copy objects",
	Flags:    copyCommandFlags,
	Before: func(c *cli.Context) error {
		if c.Args().Len() != 2 {
			return fmt.Errorf("expected source and destination arguments")
		}

		dst, err := url.New(c.Args().Get(1))
		if err != nil {
			return err
		}

		if dst.HasGlob() {
			return fmt.Errorf("target %q can not contain glob characters", dst)
		}

		if c.Int64("part-size") < 5 {
			return fmt.Errorf("part size should be greater than 5 MiB")
		}

		if c.Int("concurrency") < 1 {
			return fmt.Errorf("copy concurrency should be greater than 1")
		}

		return nil
	},
	Action: func(c *cli.Context) error {
		copyCommand := Copy{
			src:          c.Args().Get(0),
			dst:          c.Args().Get(1),
			op:           c.Command.Name,
			fullCommand:  givenCommand(c),
			deleteSource: false, // don't delete source
			// flags
			noClobber:     c.Bool("no-clobber"),
			ifSizeDiffer:  c.Bool("if-size-differ"),
			ifSourceNewer: c.Bool("if-source-newer"),
			recursive:     c.Bool("recursive"),
			parents:       c.Bool("parents"),
			storageClass:  storage.LookupClass(c.String("storage-class")),
			concurrency:   c.Int("concurrency"),
			partSize:      c.Int64("partSize") * megabytes,
		}

		return copyCommand.Run(c.Context)
	},
}

type Copy struct {
	src         string
	dst         string
	op          string
	fullCommand string

	deleteSource bool

	// flags
	noClobber     bool
	ifSizeDiffer  bool
	ifSourceNewer bool
	recursive     bool
	parents       bool
	storageClass  storage.StorageClass

	// s3 options
	concurrency int
	partSize    int64
}

func (c Copy) Run(ctx context.Context) error {
	origSrc, err := url.New(c.src)
	if err != nil {
		return err
	}

	dsturl, err := url.New(c.dst)
	if err != nil {
		return err
	}

	// set recursive=true for local->remote copy operations. this
	// is required for backwards compatibility.
	recursive := c.recursive || (!origSrc.IsRemote() && dsturl.IsRemote())

	objch, err := expandSource(ctx, origSrc, recursive)
	if err != nil {
		return err
	}

	waiter := parallel.NewWaiter()

	var (
		merror    error
		errDoneCh = make(chan bool)
	)
	go func() {
		defer close(errDoneCh)
		for err := range waiter.Err() {
			merror = multierror.Append(merror, err)
		}
	}()

	for object := range objch {
		if object.Type.IsDir() || errorpkg.IsCancelation(object.Err) {
			continue
		}

		if err := object.Err; err != nil {
			printError(c.fullCommand, c.op, err)
			continue
		}

		srcurl := object.URL
		var task parallel.Task

		switch {
		case srcurl.Type == dsturl.Type: // local->local or remote->remote
			task = c.prepareCopyTask(ctx, origSrc, srcurl, dsturl)
		case srcurl.IsRemote(): // remote->local
			task = c.prepareDownloadTask(ctx, origSrc, srcurl, dsturl)
		case dsturl.IsRemote(): // local->remote
			task = c.prepareUploadTask(ctx, srcurl, dsturl)
		default:
			panic("unexpected src-dst pair")
		}

		parallel.Run(task, waiter)
	}

	waiter.Wait()
	<-errDoneCh

	return merror
}

func (c Copy) prepareCopyTask(
	ctx context.Context,
	origSrc *url.URL,
	srcurl *url.URL,
	dsturl *url.URL,
) func() error {
	return func() error {
		dsturl, err := prepareCopyDestination(ctx, origSrc, srcurl, dsturl, c.parents)
		if err != nil {
			return err
		}

		err = c.doCopy(ctx, srcurl, dsturl)
		if err != nil {
			return &errorpkg.Error{
				Op:  c.op,
				Src: srcurl,
				Dst: dsturl,
				Err: err,
			}
		}
		return nil
	}
}

func (c Copy) prepareDownloadTask(
	ctx context.Context,
	origSrc *url.URL,
	srcurl *url.URL,
	dsturl *url.URL,
) func() error {
	return func() error {
		dsturl, err := prepareDownloadDestination(ctx, origSrc, srcurl, dsturl, c.parents)
		if err != nil {
			return err
		}

		err = c.doDownload(ctx, srcurl, dsturl)
		if err != nil {
			return &errorpkg.Error{
				Op:  c.op,
				Src: srcurl,
				Dst: dsturl,
				Err: err,
			}
		}
		return nil
	}
}

func (c Copy) prepareUploadTask(
	ctx context.Context,
	srcurl *url.URL,
	dsturl *url.URL,
) func() error {
	return func() error {
		dsturl := prepareUploadDestination(srcurl, dsturl, c.parents)

		err := c.doUpload(ctx, srcurl, dsturl)
		if err != nil {
			return &errorpkg.Error{
				Op:  c.op,
				Src: srcurl,
				Dst: dsturl,
				Err: err,
			}
		}
		return nil
	}
}

// doDownload is used to fetch a remote object and save as a local object.
func (c Copy) doDownload(ctx context.Context, srcurl *url.URL, dsturl *url.URL) error {
	srcClient, err := storage.NewClient(srcurl)
	if err != nil {
		return err
	}

	dstClient, err := storage.NewClient(dsturl)
	if err != nil {
		return err
	}

	err = c.shouldOverride(ctx, srcurl, dsturl)
	if err != nil {
		// FIXME(ig): rename
		if errorpkg.IsWarning(err) {
			printDebug(c.op, srcurl, dsturl, err)
			return nil
		}
		return err
	}

	f, err := os.Create(dsturl.Absolute())
	if err != nil {
		return err
	}
	defer f.Close()

	size, err := srcClient.Get(ctx, srcurl, f, c.concurrency, c.partSize)
	if err != nil {
		err = dstClient.Delete(ctx, dsturl)
	} else if c.deleteSource {
		err = srcClient.Delete(ctx, srcurl)
	}

	if err != nil {
		return err
	}

	msg := log.InfoMessage{
		Operation:   c.op,
		Source:      srcurl,
		Destination: dsturl,
		Object: &storage.Object{
			Size: size,
		},
	}
	log.Info(msg)

	return nil
}

func (c Copy) doUpload(ctx context.Context, srcurl *url.URL, dsturl *url.URL) error {
	// TODO(ig): use storage abstraction
	f, err := os.Open(srcurl.Absolute())
	if err != nil {
		return err
	}
	defer f.Close()

	err = c.shouldOverride(ctx, srcurl, dsturl)
	if err != nil {
		if errorpkg.IsWarning(err) {
			printDebug(c.op, srcurl, dsturl, err)
			return nil
		}
		return err
	}

	dstClient, err := storage.NewClient(dsturl)
	if err != nil {
		return err
	}

	metadata := map[string]string{
		"StorageClass": string(c.storageClass),
		"ContentType":  guessContentType(f),
	}

	err = dstClient.Put(ctx, f, dsturl, metadata, c.concurrency, c.partSize)
	if err != nil {
		return err
	}

	srcClient, err := storage.NewClient(srcurl)
	if err != nil {
		return err
	}

	obj, _ := srcClient.Stat(ctx, srcurl)
	size := obj.Size

	if c.deleteSource {
		if err := srcClient.Delete(ctx, srcurl); err != nil {
			return err
		}
	}

	msg := log.InfoMessage{
		Operation:   c.op,
		Source:      srcurl,
		Destination: dsturl,
		Object: &storage.Object{
			Size:         size,
			StorageClass: c.storageClass,
		},
	}
	log.Info(msg)

	return nil
}

func (c Copy) doCopy(ctx context.Context, srcurl *url.URL, dsturl *url.URL) error {
	srcClient, err := storage.NewClient(srcurl)
	if err != nil {
		return err
	}

	metadata := map[string]string{
		"StorageClass": string(c.storageClass),
	}

	err = c.shouldOverride(ctx, srcurl, dsturl)
	if err != nil {
		if errorpkg.IsWarning(err) {
			printDebug(c.op, srcurl, dsturl, err)
			return nil
		}
		return err
	}

	err = srcClient.Copy(ctx, srcurl, dsturl, metadata)
	if err != nil {
		return err
	}

	if c.deleteSource {
		if err := srcClient.Delete(ctx, srcurl); err != nil {
			return err
		}
	}

	msg := log.InfoMessage{
		Operation:   c.op,
		Source:      srcurl,
		Destination: dsturl,
		Object: &storage.Object{
			URL:          dsturl,
			StorageClass: c.storageClass,
		},
	}
	log.Info(msg)

	return nil
}

// shouldOverride function checks if the destination should be overridden if
// the source-destination pair and given copy flags conform to the
// override criteria. For example; "cp -n -s <src> <dst>" should not override
// the <dst> if <src> and <dst> filenames are the same, except if the size
// differs.
func (c Copy) shouldOverride(ctx context.Context, srcurl *url.URL, dsturl *url.URL) error {
	// if not asked to override, ignore.
	if !c.noClobber && !c.ifSizeDiffer && !c.ifSourceNewer {
		return nil
	}

	srcObj, err := getObject(ctx, srcurl)
	if err != nil {
		return err
	}

	dstObj, err := getObject(ctx, dsturl)
	if err != nil {
		return err
	}

	// if destination not exists, no conditions apply.
	if dstObj == nil {
		return nil
	}

	var stickyErr error
	if c.noClobber {
		stickyErr = errorpkg.ErrObjectExists
	}

	if c.ifSizeDiffer {
		if srcObj.Size == dstObj.Size {
			stickyErr = errorpkg.ErrObjectSizesMatch
		} else {
			stickyErr = nil
		}
	}

	if c.ifSourceNewer {
		srcMod, dstMod := srcObj.ModTime, dstObj.ModTime

		if !srcMod.After(*dstMod) {
			stickyErr = errorpkg.ErrObjectIsNewer
		} else {
			stickyErr = nil
		}
	}

	return stickyErr
}

func guessContentType(rs io.ReadSeeker) string {
	defer rs.Seek(0, io.SeekStart)

	const bufsize = 512
	buf, err := ioutil.ReadAll(io.LimitReader(rs, bufsize))
	if err != nil {
		return ""
	}

	return http.DetectContentType(buf)
}

func givenCommand(c *cli.Context) string {
	return fmt.Sprintf("%v %v", c.Command.FullName(), strings.Join(c.Args().Slice(), " "))
}

// prepareCopyDestination will return a new destination URL for local->local
// and remote->remote copy operations.
func prepareCopyDestination(
	ctx context.Context,
	origSrc *url.URL,
	srcurl *url.URL,
	dsturl *url.URL,
	parents bool,
) (*url.URL, error) {
	objname := srcurl.Base()
	if parents {
		objname = srcurl.Relative()
	}

	// For remote->remote copy operations, treat <dst> as prefix if it has "/"
	// suffix.
	if dsturl.IsRemote() {
		if strings.HasSuffix(dsturl.Path, "/") {
			dsturl = dsturl.Join(objname)
		}
		return dsturl, nil
	}

	client, err := storage.NewClient(dsturl)
	if err != nil {
		return nil, err
	}

	// For local->local copy operations, we can safely stat <dst> to check if
	// it is a file or a directory.
	obj, err := client.Stat(ctx, dsturl)
	if err != nil && err != storage.ErrGivenObjectNotFound {
		return nil, err
	}

	// Absolute <src> path is given. Use given <dst> and local copy operation
	// will create missing directories if <dst> has one.
	if !origSrc.HasGlob() {
		return dsturl, nil
	}

	// For local->local copy operations, if <src> has glob, <dst> is expected
	// to be a directory. As always, local copy operation will create missing
	// directories if <dst> has one.
	if obj != nil && !obj.Type.IsDir() {
		return nil, fmt.Errorf("destination argument is expected to be a directory")
	}

	return dsturl.Join(objname), nil
}

// prepareDownloadDestination will return a new destination URL for
// remote->local and remote->remote copy operations.
func prepareDownloadDestination(
	ctx context.Context,
	origSrc *url.URL,
	srcurl *url.URL,
	dsturl *url.URL,
	parents bool,
) (*url.URL, error) {
	objname := srcurl.Base()
	if parents {
		objname = srcurl.Relative()
	}

	if origSrc.HasGlob() {
		if err := os.MkdirAll(dsturl.Absolute(), os.ModePerm); err != nil {
			return nil, err
		}
	}

	client, err := storage.NewClient(dsturl)
	if err != nil {
		return nil, err
	}

	obj, err := client.Stat(ctx, dsturl)
	if err != nil && err != storage.ErrGivenObjectNotFound {
		return nil, err
	}

	if parents {
		if obj != nil && !obj.Type.IsDir() {
			return nil, fmt.Errorf("destination argument is expected to be a directory")
		}
		dsturl = dsturl.Join(objname)
		if err := os.MkdirAll(dsturl.Dir(), os.ModePerm); err != nil {
			return nil, err
		}
	}

	if err == storage.ErrGivenObjectNotFound {
		if err := os.MkdirAll(dsturl.Dir(), os.ModePerm); err != nil {
			return nil, err
		}
		if strings.HasSuffix(dsturl.Absolute(), "/") {
			dsturl = dsturl.Join(objname)
		}
	} else {
		if obj.Type.IsDir() {
			dsturl = obj.URL.Join(objname)
		}
	}

	return dsturl, nil
}

// prepareUploadDestination will return a new destination URL for local->remote
// operations.
func prepareUploadDestination(
	srcurl *url.URL,
	dsturl *url.URL,
	parents bool,
) *url.URL {
	// if S3 destination is not a bucket and does not end with "/",
	// use raw destination url.
	if !dsturl.IsBucket() && !strings.HasSuffix(dsturl.Absolute(), "/") {
		return dsturl
	}

	objname := srcurl.Base()
	if parents {
		objname = srcurl.Relative()
	}
	return dsturl.Join(objname)
}

// expandSource returns the full list of objects from the given src argument.
// If src is an expandable URL, such as directory, prefix or a glob, all
// objects are returned by walking the source.
func expandSource(
	ctx context.Context,
	srcurl *url.URL,
	isRecursive bool,
) (<-chan *storage.Object, error) {
	// TODO(ig): this function could be in the storage layer.

	client, err := storage.NewClient(srcurl)
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
		return client.List(ctx, srcurl, isRecursive), nil
	}

	ch := make(chan *storage.Object, 1)
	ch <- &storage.Object{URL: srcurl}
	close(ch)
	return ch, nil
}

// getObject checks if the object from given url exists. If no object is
// found, error and returning object would be nil.
func getObject(ctx context.Context, url *url.URL) (*storage.Object, error) {
	client, err := storage.NewClient(url)
	if err != nil {
		return nil, err
	}

	obj, err := client.Stat(ctx, url)
	if err == storage.ErrGivenObjectNotFound {
		return nil, nil
	}

	return obj, err
}
