package command

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/parallel"
	"github.com/peak/s5cmd/storage"
	"github.com/urfave/cli/v2"
)

func validateArguments(c *cli.Context) error {
	if c.Args().Len() != 2 {
		return fmt.Errorf("expected source and destination arguments")
	}
	return nil
}

// FIXME(ig): move
func givenCommand(c *cli.Context) string {
	return fmt.Sprintf("%v %v", c.Command.FullName(), strings.Join(c.Args().Slice(), " "))
}

var CopyCommand = &cli.Command{
	Name:     "cp",
	HelpName: "copy",
	Usage:    "TODO",
	Flags: []cli.Flag{
		&cli.BoolFlag{Name: "no-clobber", Aliases: []string{"n"}},
		&cli.BoolFlag{Name: "if-size-differ", Aliases: []string{"s"}},
		&cli.BoolFlag{Name: "if-source-newer", Aliases: []string{"u"}},
		&cli.BoolFlag{Name: "parents"},
		&cli.BoolFlag{Name: "recursive", Aliases: []string{"R"}},
		&cli.StringFlag{Name: "storage-class"},
	},
	Before: func(c *cli.Context) error {
		return validateArguments(c)
	},
	OnUsageError: func(c *cli.Context, err error, isSubcommand bool) error {
		if err != nil {
			printError(givenCommand(c), "copy", err)
		}
		return err
	},
	Action: func(c *cli.Context) error {
		noClobber := c.Bool("no-clobber")
		ifSizeDiffer := c.Bool("if-size-differ")
		ifSourceNewer := c.Bool("if-source-newer")
		recursive := c.Bool("recursive")
		parents := c.Bool("parents")
		storageClass := storage.LookupClass(c.String("storage-class"))

		return Copy(
			c.Context,
			c.Args().Get(0),
			c.Args().Get(1),
			givenCommand(c),
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
	src string,
	dst string,
	givenCommand string,
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

	srcClient, err := storage.NewClient(srcurl)
	if err != nil {
		return err
	}

	// set recursive=true for local->remote copy operations. this
	// is required for backwards compatibility.
	recursive = recursive || (!srcurl.IsRemote() && dsturl.IsRemote())

	for object := range srcClient.List(ctx, srcurl, recursive, storage.ListAllItems) {
		if err := object.Err; err != nil {
			// FIXME(ig):
			fmt.Println("ERR", err)
			continue
		}

		if object.Type.IsDir() {
			continue
		}

		src := object.URL

		var task func() error

		switch {
		case srcurl.Type == dsturl.Type: // local->local or remote->remote
			task = doCopy(
				ctx,
				src,
				dsturl,
				givenCommand,
				// flags
				false, // dont delete source
				noClobber,
				ifSizeDiffer,
				ifSourceNewer,
				parents,
				storageClass,
			)
		case srcurl.IsRemote(): // remote->local
			task = doDownload(
				ctx,
				src,
				dsturl,
				givenCommand,
				// flags
				false, // dont delete source
				noClobber,
				ifSizeDiffer,
				ifSourceNewer,
				parents,
			)
		case dsturl.IsRemote(): // local->remote
			task = doUpload(
				ctx,
				src,
				dsturl,
				givenCommand,
				// flags
				false, // dont delete source
				noClobber,
				ifSizeDiffer,
				ifSourceNewer,
				parents,
				storageClass,
			)
		default:
			panic("unexpected src-dst pair")
		}

		parallel.Run(task)
	}

	return nil
}

// doDownload is used to fetch a remote object and save as a local object.
func doDownload(
	ctx context.Context,
	src *objurl.ObjectURL,
	dst *objurl.ObjectURL,
	givenCommand string,
	// flags
	deleteSource bool,
	noClobber bool,
	ifSizeDiffer bool,
	ifSourceNewer bool,
	parents bool,
) func() error {
	return func() error {
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

		err = checkConditions(
			ctx,
			src,
			dst,
			noClobber,
			ifSizeDiffer,
			ifSourceNewer,
		)
		if err != nil {
			if isWarning(err) {
				msg := log.WarningMessage{
					Job:       fmt.Sprintf("cp %v %v", src, dst),
					Operation: "copy",
					Err:       err.Error(),
				}
				log.Warning(msg)
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

		if err != nil {
			return err
		}

		msg := log.InfoMessage{
			Operation:   "download",
			Source:      src,
			Destination: dst,
			Object: &storage.Object{
				Size: size,
			},
		}

		log.Info(msg)
		return nil
	}
}

func doUpload(
	ctx context.Context,
	src *objurl.ObjectURL,
	dst *objurl.ObjectURL,
	givenCommand string,
	// flags
	deleteSource bool,
	noClobber bool,
	ifSizeDiffer bool,
	ifSourceNewer bool,
	parents bool,
	storageClass storage.StorageClass,
) func() error {
	return func() error {
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

		err = checkConditions(
			ctx,
			src,
			dst,
			noClobber,
			ifSizeDiffer,
			ifSourceNewer,
		)
		if err != nil {
			if isWarning(err) {
				msg := log.WarningMessage{
					Job:       fmt.Sprintf("cp %v %v", src, dst),
					Operation: "copy",
					Err:       err.Error(),
				}
				log.Warning(msg)
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

		if err != nil {
			return err
		}

		msg := log.InfoMessage{
			Operation:   "upload",
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
}

func doCopy(
	ctx context.Context,
	src *objurl.ObjectURL,
	dst *objurl.ObjectURL,
	givenCommand string,
	// flags
	deleteSource bool,
	noClobber bool,
	ifSizeDiffer bool,
	ifSourceNewer bool,
	parents bool,
	storageClass storage.StorageClass,
) func() error {
	return func() error {
		client, err := storage.NewClient(src)
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

		dst = dst.Join(objname)

		err = checkConditions(
			ctx,
			src,
			dst,
			noClobber,
			ifSizeDiffer,
			ifSourceNewer,
		)
		if err != nil {
			if isWarning(err) {
				msg := log.WarningMessage{
					Job:       fmt.Sprintf("cp %v %v", src, dst),
					Operation: "copy",
					Err:       err.Error(),
				}
				log.Warning(msg)
				return nil
			}
			return err
		}

		err = client.Copy(
			ctx,
			src,
			dst,
			metadata,
		)

		if deleteSource && err == nil {
			err = client.Delete(ctx, src)
		}

		if err != nil {
			return err
		}

		// TODO(ig):
		op := "copy"
		if deleteSource {
			op = "move"
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
