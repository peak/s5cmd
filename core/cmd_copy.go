package core

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/storage"
	"github.com/urfave/cli/v2"
)

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
		if c.Args().Len() != 2 {
			return fmt.Errorf("expected source and destination arguments")
		}
		return nil
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

	if err := CheckConditions(
		ctx,
		srcurl,
		dsturl,
		noClobber,
		ifSizeDiffer,
		ifSourceNewer,
	); err != nil {
		return err
	}

	if srcurl.Type == dsturl.Type {
		return doCopy(
			ctx,
			srcurl,
			dsturl,
			false, // dont delete source
			noClobber,
			ifSizeDiffer,
			ifSourceNewer,
			recursive,
			parents,
			storageClass,
		)
	}

	if srcurl.IsRemote() {
		return doDownload(
			ctx,
			srcurl,
			dsturl,
			false, // dont delete source
			noClobber,
			ifSizeDiffer,
			ifSourceNewer,
			recursive,
			parents,
		)
	}

	return doUpload(
		ctx,
		srcurl,
		dsturl,
		false, // dont delete source
		noClobber,
		ifSizeDiffer,
		ifSourceNewer,
		recursive,
		parents,
		storageClass,
	)
}

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
) error {
	srcClient, err := storage.NewClient(src)
	if err != nil {
		return err
	}

	dstClient, err := storage.NewClient(dst)
	if err != nil {
		return err
	}

	destFilename := dst.Absolute()

	// TODO(ig): use storage abstraction
	f, err := os.Create(destFilename)
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

	log.Info(InfoMessage{
		Operation:   "download",
		Source:      src,
		Destination: dst,
		Object:      &storage.Object{Size: size},
	})

	return nil
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
) error {

	// TODO(ig): use storage abstraction
	f, err := os.Open(src.Absolute())
	if err != nil {
		return err
	}
	defer f.Close()

	dstClient, err := storage.NewClient(dst)
	if err != nil {
		return err
	}

	srcClient, err := storage.NewClient(src)
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

	log.Info(InfoMessage{
		Operation:   "upload",
		Source:      src,
		Destination: dst,
		Object:      &storage.Object{Size: size},
	})

	return nil
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
) error {
	if err := CheckConditions(
		ctx,
		src,
		dst,
		noClobber,
		ifSizeDiffer,
		ifSourceNewer,
	); err != nil {
		return err
	}

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
	msg := InfoMessage{
		Operation:   op,
		Source:      src,
		Destination: dst,
		Object: &storage.Object{
			URL:          dst,
			StorageClass: storage.StorageClass(storageClass),
		},
	}
	fmt.Println(msg)

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
