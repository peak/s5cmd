package command

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
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
		storageClass := c.String("storage-class")

		return Copy(
			c.Context,
			c.Args().Get(0),
			c.Args().Get(1),
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
	// flags
	noClobber bool,
	ifSizeDiffer bool,
	ifSourceNewer bool,
	recursive bool,
	parents bool,
	storageClass string,
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

	for object := range srcClient.List(ctx, srcurl, recursive, storage.ListAllItems) {
		if err := object.Err; err != nil {
			// FIXME(ig):
			fmt.Println("ERR", err)
			continue
		}

		src := object.URL
		if err := checkConditions(
			ctx,
			src,
			dsturl,
			noClobber,
			ifSizeDiffer,
			ifSourceNewer,
		); err != nil {
			return err
		}

		var task func() error

		switch {
		case srcurl.Type == dsturl.Type: // local->local or remote->remote
			task = doCopy(
				ctx,
				src,
				dsturl,
				false, // dont delete source
				noClobber,
				ifSizeDiffer,
				ifSourceNewer,
				recursive,
				parents,
				storageClass,
			)
		case srcurl.IsRemote(): // remote->local
			task = doDownload(
				ctx,
				src,
				dsturl,
				false, // dont delete source
				noClobber,
				ifSizeDiffer,
				ifSourceNewer,
				recursive,
				parents,
			)
		case dsturl.IsRemote(): // local->remote
			task = doUpload(
				ctx,
				src,
				dsturl,
				false, // dont delete source
				noClobber,
				ifSizeDiffer,
				ifSourceNewer,
				recursive,
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
	// flags
	deleteSource bool,
	noClobber bool,
	ifSizeDiffer bool,
	ifSourceNewer bool,
	recursive bool,
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

		joinpath := src.Base()
		if parents {
			joinpath = src.Relative()
		}

		localdst := dst.Join(joinpath)
		dir := filepath.Dir(localdst.Absolute())
		os.MkdirAll(dir, os.ModePerm)

		// TODO(ig): use storage abstraction
		f, err := os.Create(localdst.Absolute())
		if err != nil {
			return err
		}
		defer f.Close()

		size, err := srcClient.Get(ctx, src, f)
		if err != nil {
			err = dstClient.Delete(ctx, localdst)
		} else if deleteSource {
			err = srcClient.Delete(ctx, src)
		}

		if err != nil {
			return err
		}

		msg := log.InfoMessage{
			Operation:   "download",
			Source:      src,
			Destination: localdst,
			Object:      &storage.Object{Size: size},
		}

		log.Info(msg)
		return nil
	}
}

func doUpload(
	ctx context.Context,
	src *objurl.ObjectURL,
	dst *objurl.ObjectURL,
	// flags
	deleteSource bool,
	noClobber bool,
	ifSizeDiffer bool,
	ifSourceNewer bool,
	recursive bool,
	parents bool,
	storageClass string,
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

		trimPrefix := src.Absolute()
		trimPrefix = filepath.Dir(trimPrefix)
		if trimPrefix == "." {
			trimPrefix = ""
		} else {
			trimPrefix += string(filepath.Separator)
		}

		objname := src.Base()
		if parents {
			objname = src.Relative()
		}

		dst = dst.Join(objname)

		dstClient, err := storage.NewClient(dst)
		if err != nil {
			return err
		}

		metadata := map[string]string{
			"StorageClass": storageClass,
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
			Object:      &storage.Object{Size: size},
		}
		log.Info(msg)

		return nil
	}
}

func doCopy(
	ctx context.Context,
	src *objurl.ObjectURL,
	dst *objurl.ObjectURL,
	// flags
	deleteSource bool,
	noClobber bool,
	ifSizeDiffer bool,
	ifSourceNewer bool,
	recursive bool,
	parents bool,
	storageClass string,
) func() error {
	return func() error {
		client, err := storage.NewClient(src)
		if err != nil {
			return err
		}

		metadata := map[string]string{
			"StorageClass": storageClass,
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
