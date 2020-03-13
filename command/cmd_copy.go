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

// FIXME(ig): move
func givenCommand(c *cli.Context) string {
	return fmt.Sprintf("%v %v", c.Command.FullName(), strings.Join(c.Args().Slice(), " "))
}

// shouldOverrideDst is a closure to check if the destination should be
// overriden if the source-destination pair and given copy flags conform to the
// override criteria. For example; "cp -n -s <src> <dst>" should not override
// the <dst> if <src> and <dst> filenames are the same, except if the size
// differs.
type shouldOverrideDst func(dst *objurl.ObjectURL) error

var copyCommandFlags = []cli.Flag{
	&cli.BoolFlag{Name: "no-clobber", Aliases: []string{"n"}},
	&cli.BoolFlag{Name: "if-size-differ", Aliases: []string{"s"}},
	&cli.BoolFlag{Name: "if-source-newer", Aliases: []string{"u"}},
	&cli.BoolFlag{Name: "parents"},
	&cli.BoolFlag{Name: "recursive", Aliases: []string{"R"}},
	&cli.StringFlag{Name: "storage-class"},
}

var CopyCommand = &cli.Command{
	Name:     "cp",
	HelpName: "copy",
	Usage:    "TODO",
	Flags:    copyCommandFlags,
	Before: func(c *cli.Context) error {
		validate := func() error {
			if c.Args().Len() != 2 {
				return fmt.Errorf("expected source and destination arguments")
			}

			dst, err := objurl.New(c.Args().Get(1))
			if err != nil {
				return err
			}

			if dst.HasGlob() {
				return fmt.Errorf("target %q can not contain glob characters", dst)
			}

			return nil
		}
		if err := validate(); err != nil {
			printError(givenCommand(c), c.Command.Name, err)
			return err
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

		err := Copy(
			c.Context,
			c.Args().Get(0),
			c.Args().Get(1),
			c.Command.Name,
			false, // don't delete source
			// flags
			noClobber,
			ifSizeDiffer,
			ifSourceNewer,
			recursive,
			parents,
			storageClass,
		)
		if err != nil {
			printError(givenCommand(c), c.Command.Name, err)
			return err
		}

		return nil
	},
}

func expandSource(ctx context.Context, src *objurl.ObjectURL, isRecursive bool) <-chan *storage.Object {
	// FIXME: handle errors
	client, _ := storage.NewClient(src)
	isDir := false

	if !src.HasGlob() && !src.IsRemote() {
		obj, _ := client.Stat(ctx, src)
		isDir = obj.Type.IsDir()
	}

	if src.HasGlob() || isDir {
		return client.List(ctx, src, isRecursive, storage.ListAllItems)
	}

	ch := make(chan *storage.Object, 1)
	ch <- &storage.Object{URL: src}
	close(ch)
	return ch
}

func prepareDownloadDestination(
	ctx context.Context,
	originalSrc *objurl.ObjectURL,
	src *objurl.ObjectURL,
	dst *objurl.ObjectURL,
	parents bool,
) (*objurl.ObjectURL, error) {
	dstClient, err := storage.NewClient(dst)
	if err != nil {
		return nil, err
	}

	if originalSrc.HasGlob() {
		os.MkdirAll(dst.Absolute(), os.ModePerm)
	}

	obj, err := dstClient.Stat(ctx, dst)
	if err != nil && err != storage.ErrGivenObjectNotFound {
		return nil, err
	}

	objname := src.Base()
	if parents {
		if obj != nil && !obj.Type.IsDir() {
			return nil, fmt.Errorf("destination argument is expected to be a directory")
		}
		objname = src.Relative()
		dst = dst.Join(objname)
		os.MkdirAll(dst.Dir(), os.ModePerm)
	}

	if err == storage.ErrGivenObjectNotFound {
		// TODO(ig): use storage abstraction
		os.MkdirAll(dst.Dir(), os.ModePerm)
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

func prepareCopyDestination(
	ctx context.Context,
	originalSrc *objurl.ObjectURL,
	src *objurl.ObjectURL,
	dst *objurl.ObjectURL,
	parents bool,
) (*objurl.ObjectURL, error) {
	dstClient, err := storage.NewClient(dst)
	if err != nil {
		return nil, err
	}

	objname := src.Base()
	if parents {
		objname = src.Relative()
	}

	// FIXME(ig):
	if !dst.IsRemote() {
		obj, err := dstClient.Stat(ctx, dst)
		if err != nil && err != storage.ErrGivenObjectNotFound {
			return nil, err
		}
		if originalSrc.HasGlob() {
			if obj != nil && !obj.Type.IsDir() {
				return nil, fmt.Errorf("destination argument is expected to be a directory")
			}
			dst = dst.Join(objname)
			os.MkdirAll(dst.Dir(), os.ModePerm)
		}
	} else {
		if strings.HasSuffix(dst.Path, "/") {
			dst = dst.Join(objname)
		}
	}

	return dst, nil
}

func Copy(
	ctx context.Context,
	src string,
	dst string,
	op string,
	deleteSource bool,
	// flags
	noClobber bool,
	ifSizeDiffer bool,
	ifSourceNewer bool,
	recursive bool,
	parents bool,
	storageClass storage.StorageClass,
) error {
	srcurl, err := objurl.New(src)
	if err != nil {
		return err
	}

	dsturl, err := objurl.New(dst)
	if err != nil {
		return err
	}

	// set recursive=true for local->remote copy operations. this
	// is required for backwards compatibility.
	recursive = recursive || (!srcurl.IsRemote() && dsturl.IsRemote())

	waiter := parallel.NewWaiter()

	var merror error
	go func() {
		for err := range waiter.Err() {
			merror = multierror.Append(merror, err)
		}
	}()

	for object := range expandSource(ctx, srcurl, recursive) {
		if err := object.Err; err != nil {
			// FIXME(ig):
			fmt.Println("ERR", err)
			continue
		}

		if object.Type.IsDir() {
			continue
		}

		src := object.URL

		shouldOverride := func(dst *objurl.ObjectURL) error {
			// FIXME(ig): shouldOverrideDestination
			return checkConditions(
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
					shouldOverride,
					// flags
					parents,
					storageClass,
				)
				if err != nil {
					return &errorpkg.Error{
						Op:       op,
						Src:      src,
						Dst:      dsturl,
						Original: err,
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
					shouldOverride,
					// flags
					parents,
				)

				if err != nil {
					return &errorpkg.Error{
						Op:       op,
						Src:      src,
						Dst:      dsturl,
						Original: err,
					}
				}
				return nil
			}
		case dsturl.IsRemote(): // local->remote
			task = func() error {
				err := doUpload(
					ctx,
					src,
					dsturl,
					op,
					deleteSource,
					shouldOverride,
					// flags
					parents,
					storageClass,
				)
				if err != nil {
					return &errorpkg.Error{
						Op:       op,
						Src:      src,
						Dst:      dsturl,
						Original: err,
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

	return merror
}

// doDownload is used to fetch a remote object and save as a local object.
func doDownload(
	ctx context.Context,
	src *objurl.ObjectURL,
	dst *objurl.ObjectURL,
	op string,
	deleteSource bool,
	shouldOverride shouldOverrideDst,
	// flags
	parents bool,
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
			printDebug(fullCommand(op, src, dst), op, err)
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

	if err == nil {
		// FIXME(ig): move this to parallel.Result
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

	return err
}

func doUpload(
	ctx context.Context,
	src *objurl.ObjectURL,
	dst *objurl.ObjectURL,
	op string,
	deleteSource bool,
	shouldOverride shouldOverrideDst,
	// flags
	parents bool,
	storageClass storage.StorageClass,
) error {
	// TODO(ig): use storage abstraction
	f, err := os.Open(src.Absolute())
	if err != nil {
		return err
	}
	defer f.Close()

	objname := src.Base()
	if parents {
		objname = src.Relative()
	}

	dst = dst.Join(objname)

	err = shouldOverride(dst)
	if err != nil {
		if isWarning(err) {
			printDebug(fullCommand(op, src, dst), op, err)
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

	if deleteSource && err == nil {
		err = srcClient.Delete(ctx, src)
	}

	if err == nil {
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

	return err
}

func doCopy(
	ctx context.Context,
	src *objurl.ObjectURL,
	dst *objurl.ObjectURL,
	op string,
	deleteSource bool,
	shouldOverride shouldOverrideDst,
	// flags
	parents bool,
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
			printDebug(fullCommand(op, src, dst), op, err)
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

	if deleteSource && err == nil {
		err = srcClient.Delete(ctx, src)
	}

	if err == nil {
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

	return err
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

func fullCommand(op string, src, dst *objurl.ObjectURL) string {
	return fmt.Sprintf("%v %v %v", op, src, dst)
}
