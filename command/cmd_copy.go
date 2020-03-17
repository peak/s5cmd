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
	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/parallel"
	"github.com/peak/s5cmd/storage"
)

// shouldOverrideFunc is a helper closure for shouldOverride function.
type shouldOverrideFunc func(dst *objurl.ObjectURL) error

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
}

var CopyCommand = &cli.Command{
	Name:     "cp",
	HelpName: "copy",
	Usage:    "copy objects",
	Flags:    copyCommandFlags,
	Before: func(c *cli.Context) error {
		if c.Args().Len() < 2 {
			return fmt.Errorf("expected source and destination arguments")
		}

		args := c.Args().Slice()
		last := c.Args().Len() - 1
		src := args[:last]
		dst := args[last]

		dsturl, err := objurl.New(dst)
		if err != nil {
			return err
		}

		if dsturl.HasGlob() {
			return fmt.Errorf("target %q can not contain glob characters", dst)
		}

		if err := checkSources(src...); err != nil {
			return err
		}

		if dsturl.IsRemote() && len(src) > 1 && !strings.HasSuffix(dsturl.Absolute(), "/") {
			return fmt.Errorf("target should be a directory for multiple sources")
		}

		return nil
	},
	Action: func(c *cli.Context) error {
		noClobber := c.Bool("no-clobber")
		ifSizeDiffer := c.Bool("if-size-differ")
		ifSourceNewer := c.Bool("if-source-newer")
		recursive := c.Bool("recursive")
		parents := c.Bool("parents")
		storageClass := storage.LookupClass(c.String("storage-class"))

		args := c.Args().Slice()
		last := c.Args().Len() - 1
		src := args[:last]
		dst := args[last]

		return Copy(
			c.Context,
			src,
			dst,
			c.Command.Name,
			givenCommand(c),
			false, // don't delete source
			// flags
			noClobber,
			ifSizeDiffer,
			ifSourceNewer,
			recursive,
			parents,
			storageClass,
		)
	},
}

func Copy(
	ctx context.Context,
	src []string,
	dst string,
	op string,
	fullCommand string,
	deleteSource bool,
	// flags
	noClobber bool,
	ifSizeDiffer bool,
	ifSourceNewer bool,
	recursive bool,
	parents bool,
	storageClass storage.StorageClass,
) error {
	srcurls, err := newSources(src...)
	if err != nil {
		return err
	}

	dsturl, err := objurl.New(dst)
	if err != nil {
		return err
	}

	args, err := expandSources(ctx, recursive, dsturl, srcurls...)
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

	for arg := range args {
		srcurl := arg.originalUrl
		object := arg.obj

		if object.Type.IsDir() || errorpkg.IsCancelation(object.Err) {
			continue
		}

		if err := object.Err; err != nil {
			printError(fullCommand, op, err)
			continue
		}

		src := arg.obj.URL

		shouldOverrideFunc := func(dst *objurl.ObjectURL) error {
			return shouldOverride(
				ctx,
				src,
				dst,
				noClobber,
				ifSizeDiffer,
				ifSourceNewer,
			)
		}

		var task parallel.Task

		switch {
		case srcurl.Type == dsturl.Type: // local->local or remote->remote
			task = func() error {
				dsturl, err := prepareCopyDestination(ctx, srcurl, src, dsturl, parents)
				if err != nil {
					return err
				}

				err = doCopy(
					ctx,
					src,
					dsturl,
					op,
					deleteSource,
					shouldOverrideFunc,
					// flags
					storageClass,
				)
				if err != nil {
					return &errorpkg.Error{
						Op:  op,
						Src: src,
						Dst: dsturl,
						Err: err,
					}
				}
				return nil
			}
		case srcurl.IsRemote(): // remote->local
			task = func() error {
				dsturl, err := prepareDownloadDestination(ctx, srcurl, src, dsturl, parents)
				if err != nil {
					return err
				}

				err = doDownload(
					ctx,
					src,
					dsturl,
					op,
					deleteSource,
					shouldOverrideFunc,
				)

				if err != nil {
					return &errorpkg.Error{
						Op:  op,
						Src: src,
						Dst: dsturl,
						Err: err,
					}
				}
				return nil
			}
		case dsturl.IsRemote(): // local->remote
			task = func() error {
				dsturl := prepareUploadDestination(src, dsturl, parents)

				err := doUpload(
					ctx,
					src,
					dsturl,
					op,
					deleteSource,
					shouldOverrideFunc,
					// flags
					storageClass,
				)
				if err != nil {
					return &errorpkg.Error{
						Op:  op,
						Src: src,
						Dst: dsturl,
						Err: err,
					}
				}
				return nil
			}
		default:
			panic("unexpected src-dst pair")
		}

		parallel.Run(task, waiter)
	}

	waiter.Wait()
	<-errDoneCh

	return merror
}

// doDownload is used to fetch a remote object and save as a local object.
func doDownload(
	ctx context.Context,
	src *objurl.ObjectURL,
	dst *objurl.ObjectURL,
	op string,
	deleteSource bool,
	shouldOverride shouldOverrideFunc,
) error {
	srcClient, err := storage.NewClient(src)
	if err != nil {
		return err
	}

	dstClient, err := storage.NewClient(dst)
	if err != nil {
		return err
	}

	err = shouldOverride(dst)
	if err != nil {
		// FIXME(ig): rename
		if isWarning(err) {
			printDebug(op, src, dst, err)
			return nil
		}
		return err
	}

	f, err := os.Create(dst.Absolute())
	if err != nil {
		return err
	}
	defer f.Close()

	size, err := srcClient.Get(ctx, src, f)
	if err != nil {
		err = dstClient.Delete(ctx, dst)
	} else if deleteSource {
		err = srcClient.Delete(ctx, src)
	}

	if err != nil {
		return err
	}

	msg := log.InfoMessage{
		Operation:   op,
		Source:      src,
		Destination: dst,
		Object: &storage.Object{
			Size: size,
		},
	}
	log.Info(msg)

	return nil
}

func doUpload(
	ctx context.Context,
	src *objurl.ObjectURL,
	dst *objurl.ObjectURL,
	op string,
	deleteSource bool,
	shouldOverride shouldOverrideFunc,
	// flags
	storageClass storage.StorageClass,
) error {
	// TODO(ig): use storage abstraction
	f, err := os.Open(src.Absolute())
	if err != nil {
		return err
	}
	defer f.Close()

	err = shouldOverride(dst)
	if err != nil {
		if isWarning(err) {
			printDebug(op, src, dst, err)
			return nil
		}
		return err
	}

	dstClient, err := storage.NewClient(dst)
	if err != nil {
		return err
	}

	metadata := map[string]string{
		"StorageClass": string(storageClass),
		"ContentType":  guessContentType(f),
	}

	err = dstClient.Put(
		ctx,
		f,
		dst,
		metadata,
	)
	if err != nil {
		return err
	}

	srcClient, err := storage.NewClient(src)
	if err != nil {
		return err
	}

	obj, _ := srcClient.Stat(ctx, src)
	size := obj.Size

	if deleteSource {
		if err := srcClient.Delete(ctx, src); err != nil {
			return err
		}
	}

	msg := log.InfoMessage{
		Operation:   op,
		Source:      src,
		Destination: dst,
		Object: &storage.Object{
			Size:         size,
			StorageClass: storageClass,
		},
	}
	log.Info(msg)

	return nil
}

func doCopy(
	ctx context.Context,
	src *objurl.ObjectURL,
	dst *objurl.ObjectURL,
	op string,
	deleteSource bool,
	shouldOverride shouldOverrideFunc,
	// flags
	storageClass storage.StorageClass,
) error {
	srcClient, err := storage.NewClient(src)
	if err != nil {
		return err
	}

	metadata := map[string]string{
		"StorageClass": string(storageClass),
	}

	err = shouldOverride(dst)
	if err != nil {
		if isWarning(err) {
			printDebug(op, src, dst, err)
			return nil
		}
		return err
	}

	err = srcClient.Copy(
		ctx,
		src,
		dst,
		metadata,
	)
	if err != nil {
		return err
	}

	if deleteSource {
		if err := srcClient.Delete(ctx, src); err != nil {
			return err
		}
	}

	msg := log.InfoMessage{
		Operation:   op,
		Source:      src,
		Destination: dst,
		Object: &storage.Object{
			URL:          dst,
			StorageClass: storage.StorageClass(storageClass),
		},
	}
	log.Info(msg)

	return nil
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
	originalSrc *objurl.ObjectURL,
	src *objurl.ObjectURL,
	dst *objurl.ObjectURL,
	parents bool,
) (*objurl.ObjectURL, error) {
	objname := src.Base()
	if parents {
		objname = src.Relative()
	}

	// For remote->remote copy operations, treat <dst> as prefix if it has "/"
	// suffix.
	if dst.IsRemote() {
		if strings.HasSuffix(dst.Path, "/") {
			dst = dst.Join(objname)
		}
		return dst, nil
	}

	client, err := storage.NewClient(dst)
	if err != nil {
		return nil, err
	}

	// For local->local copy operations, we can safely stat <dst> to check if
	// it is a file or a directory.
	obj, err := client.Stat(ctx, dst)
	if err != nil && err != storage.ErrGivenObjectNotFound {
		return nil, err
	}

	// Absolute <src> path is given. Use given <dst> and local copy operation
	// will create missing directories if <dst> has one.
	if !originalSrc.HasGlob() {
		return dst, nil
	}

	// For local->local copy operations, if <src> has glob, <dst> is expected
	// to be a directory. As always, local copy operation will create missing
	// directories if <dst> has one.
	if obj != nil && !obj.Type.IsDir() {
		return nil, fmt.Errorf("destination argument is expected to be a directory")
	}

	return dst.Join(objname), nil
}

// prepareDownloadDestination will return a new destination URL for
// remote->local and remote->remote copy operations.
func prepareDownloadDestination(
	ctx context.Context,
	originalSrc *objurl.ObjectURL,
	src *objurl.ObjectURL,
	dst *objurl.ObjectURL,
	parents bool,
) (*objurl.ObjectURL, error) {
	objname := src.Base()
	if parents {
		objname = src.Relative()
	}

	if originalSrc.HasGlob() {
		if err := os.MkdirAll(dst.Absolute(), os.ModePerm); err != nil {
			return nil, err
		}
	}

	client, err := storage.NewClient(dst)
	if err != nil {
		return nil, err
	}

	obj, err := client.Stat(ctx, dst)
	if err != nil && err != storage.ErrGivenObjectNotFound {
		return nil, err
	}

	if parents {
		if obj != nil && !obj.Type.IsDir() {
			return nil, fmt.Errorf("destination argument is expected to be a directory")
		}
		dst = dst.Join(objname)
		if err := os.MkdirAll(dst.Dir(), os.ModePerm); err != nil {
			return nil, err
		}
	}

	if err == storage.ErrGivenObjectNotFound {
		if err := os.MkdirAll(dst.Dir(), os.ModePerm); err != nil {
			return nil, err
		}
		if strings.HasSuffix(dst.Absolute(), "/") {
			dst = dst.Join(objname)
		}
	} else {
		if obj.Type.IsDir() {
			dst = obj.URL.Join(objname)
		}
	}

	return dst, nil
}

// prepareUploadDestination will return a new destination URL for local->remote
// operations.
func prepareUploadDestination(
	src *objurl.ObjectURL,
	dst *objurl.ObjectURL,
	parents bool,
) *objurl.ObjectURL {
	objname := src.Base()
	if parents {
		objname = src.Relative()
	}
	return dst.Join(objname)
}
