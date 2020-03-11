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

type checkFunc func(*objurl.ObjectURL) error

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
	if src.HasGlob() {
		// FIXME(ig):
		client, _ := storage.NewClient(src)

		return client.List(ctx, src, isRecursive, storage.ListAllItems)
	}

	ch := make(chan *storage.Object, 1)
	ch <- &storage.Object{URL: src}
	close(ch)
	return ch
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

		checkFunc := func(dst *objurl.ObjectURL) error {
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
				err := doCopy(
					ctx,
					src,
					dsturl,
					op,
					deleteSource,
					checkFunc,
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
				err := doDownload(
					ctx,
					src,
					dsturl,
					op,
					deleteSource,
					checkFunc,
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
					checkFunc,
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
	checkFunc checkFunc,
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

	objname := src.Base()
	if parents {
		objname = src.Relative()
	}

	dst = dst.Join(objname)

	err = checkFunc(dst)
	if err != nil {
		// FIXME(ig): rename
		if isWarning(err) {
			printDebug(fullCommand(op, src, dst), op, err)
			return nil
		}
		return err
	}

	// TODO(ig): use storage abstraction
	os.MkdirAll(dst.Dir(), os.ModePerm)
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
	checkFunc checkFunc,
	// flags
	parents bool,
	storageClass storage.StorageClass,
) error {
	srcClient, err := storage.NewClient(src)
	if err != nil {
		return err
	}

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

	err = checkFunc(dst)
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
	checkFunc checkFunc,
	// flags
	parents bool,
	storageClass storage.StorageClass,
) error {
	srcClient, err := storage.NewClient(src)
	if err != nil {
		return err
	}

	dstClient, err := storage.NewClient(dst)
	if err != nil {
		return err
	}

	metadata := map[string]string{
		"StorageClass": string(storageClass),
	}

	objname := src.Base()
	if parents {
		objname = src.Relative()
	}

	// FIXME(ig):
	if !dst.IsRemote() {
		dstObj, _ := dstClient.Stat(ctx, dst)
		if dstObj != nil && dstObj.Type.IsDir() {
			dst = dst.Join(objname)
		}
	} else {
		dstPath := fmt.Sprintf("s3://%v/%v%v", dst.Bucket, dst.Path, objname)
		dst, _ = objurl.New(dstPath)

	}

	err = checkFunc(dst)
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
