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

const (
	defaultCopyConcurrency = 5
	defaultPartSize        = 50 // MiB
	megabytes              = 1024 * 1024
)

var copyHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] source destination

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	01. Download an S3 object to working directory
		 > s5cmd {{.HelpName}} s3://bucket/prefix/object.gz .

	02. Download an S3 object and rename
		 > s5cmd {{.HelpName}} s3://bucket/prefix/object.gz myobject.gz

	03. Download all S3 objects to a directory
		 > s5cmd {{.HelpName}} s3://bucket/* target-directory/

	04. Upload a file to S3 bucket
		 > s5cmd {{.HelpName}} myfile.gz s3://bucket/

	05. Upload matching files to S3 bucket
		 > s5cmd {{.HelpName}} dir/*.gz s3://bucket/

	06. Upload all files in a directory to S3 bucket recursively
		 > s5cmd {{.HelpName}} dir/ s3://bucket/

	07. Copy S3 object to another bucket
		 > s5cmd {{.HelpName}} s3://bucket/object s3://target-bucket/prefix/object

	08. Copy matching S3 objects to another bucket
		 > s5cmd {{.HelpName}} s3://bucket/*.gz s3://target-bucket/prefix/

	09. Mirror a directory to target S3 prefix
		 > s5cmd {{.HelpName}} -n -s -u dir/ s3://bucket/target-prefix/

	10. Mirror an S3 prefix to target S3 prefix
		 > s5cmd {{.HelpName}} -n -s -u s3://bucket/source-prefix/* s3://bucket/target-prefix/
`

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
		Name:    "flatten",
		Aliases: []string{"f"},
		Usage:   "flatten directory structure of source, starting from the first wildcard",
	},
	&cli.BoolFlag{
		Name:  "no-follow-symlinks",
		Usage: "do not follow symbolic links",
	},
	&cli.StringFlag{
		Name:  "storage-class",
		Usage: "set storage class for target ('STANDARD','REDUCED_REDUNDANCY','GLACIER','STANDARD_IA','ONEZONE_IA','INTELLIGENT_TIERING','DEEP_ARCHIVE')",
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

var copyCommand = &cli.Command{
	Name:               "cp",
	HelpName:           "cp",
	Usage:              "copy objects",
	Flags:              copyCommandFlags,
	CustomHelpTemplate: copyHelpTemplate,
	Before: func(c *cli.Context) error {
		return validate(c)
	},
	Action: func(c *cli.Context) error {
		return Copy{
			src:          c.Args().Get(0),
			dst:          c.Args().Get(1),
			op:           c.Command.Name,
			fullCommand:  givenCommand(c),
			deleteSource: false, // don't delete source
			// flags
			noClobber:      c.Bool("no-clobber"),
			ifSizeDiffer:   c.Bool("if-size-differ"),
			ifSourceNewer:  c.Bool("if-source-newer"),
			flatten:        c.Bool("flatten"),
			followSymlinks: !c.Bool("no-follow-symlinks"),
			storageClass:   storage.StorageClass(c.String("storage-class")),
			concurrency:    c.Int("concurrency"),
			partSize:       c.Int64("part-size") * megabytes,
		}.Run(c.Context)
	},
}

// Copy holds copy operation flags and states.
type Copy struct {
	src         string
	dst         string
	op          string
	fullCommand string

	deleteSource bool

	// flags
	noClobber      bool
	ifSizeDiffer   bool
	ifSourceNewer  bool
	flatten        bool
	followSymlinks bool
	storageClass   storage.StorageClass

	// s3 options
	concurrency int
	partSize    int64
}

// Run starts copying given source objects to destination.
func (c Copy) Run(ctx context.Context) error {
	srcurl, err := url.New(c.src)
	if err != nil {
		return err
	}

	dsturl, err := url.New(c.dst)
	if err != nil {
		return err
	}

	client := storage.NewClient(srcurl)

	objch, err := expandSource(ctx, client, c.followSymlinks, srcurl)
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
			if strings.Contains(err.Error(), "too many open files") {
				fmt.Println("WARNING You are hitting the max open file limit allowed by your OS. You can increase open file limit or try to decrease the number of workers with the parameter '-numworkers'")
				fmt.Printf("ERROR %v\n", err)

				os.Exit(1)
			}
			merror = multierror.Append(merror, err)
		}
	}()

	isBatch := srcurl.HasGlob()
	if !isBatch && !srcurl.IsRemote() {
		obj, _ := client.Stat(ctx, srcurl)
		isBatch = obj != nil && obj.Type.IsDir()
	}

	for object := range objch {
		if object.Type.IsDir() || errorpkg.IsCancelation(object.Err) {
			continue
		}

		if err := object.Err; err != nil {
			printError(c.fullCommand, c.op, err)
			continue
		}

		if object.StorageClass.IsGlacier() {
			err := fmt.Errorf("object '%v' is on Glacier storage", object)
			printError(c.fullCommand, c.op, err)
			continue
		}

		srcurl := object.URL
		var task parallel.Task

		switch {
		case srcurl.Type == dsturl.Type: // local->local or remote->remote
			task = c.prepareCopyTask(ctx, srcurl, dsturl, isBatch)
		case srcurl.IsRemote(): // remote->local
			task = c.prepareDownloadTask(ctx, srcurl, dsturl, isBatch)
		case dsturl.IsRemote(): // local->remote
			task = c.prepareUploadTask(ctx, srcurl, dsturl, isBatch)
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
	srcurl *url.URL,
	dsturl *url.URL,
	isBatch bool,
) func() error {
	return func() error {
		dsturl = prepareRemoteDestination(srcurl, dsturl, c.flatten, isBatch)
		err := c.doCopy(ctx, srcurl, dsturl)
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
	srcurl *url.URL,
	dsturl *url.URL,
	isBatch bool,
) func() error {
	return func() error {
		dsturl, err := prepareLocalDestination(ctx, srcurl, dsturl, c.flatten, isBatch)
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
	isBatch bool,
) func() error {
	return func() error {
		dsturl = prepareRemoteDestination(srcurl, dsturl, c.flatten, isBatch)
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
	srcClient := storage.NewClient(srcurl)
	dstClient := storage.NewClient(dsturl)

	err := c.shouldOverride(ctx, srcurl, dsturl)
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
		_ = dstClient.Delete(ctx, dsturl)
		return err
	}

	if c.deleteSource {
		_ = srcClient.Delete(ctx, srcurl)
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

	dstClient := storage.NewClient(dsturl)

	metadata := map[string]string{
		"StorageClass": string(c.storageClass),
		"ContentType":  guessContentType(f),
	}

	err = dstClient.Put(ctx, f, dsturl, metadata, c.concurrency, c.partSize)
	if err != nil {
		return err
	}

	srcClient := storage.NewClient(srcurl)

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
	srcClient := storage.NewClient(srcurl)

	metadata := map[string]string{
		"StorageClass": string(c.storageClass),
	}

	err := c.shouldOverride(ctx, srcurl, dsturl)
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

// prepareRemoteDestination will return a new destination URL for
// remote->remote and local->remote copy operations.
func prepareRemoteDestination(
	srcurl *url.URL,
	dsturl *url.URL,
	flatten bool,
	isBatch bool,
) *url.URL {
	objname := srcurl.Base()
	if isBatch && !flatten {
		objname = srcurl.Relative()
	}

	if dsturl.IsPrefix() || dsturl.IsBucket() {
		dsturl = dsturl.Join(objname)
	}
	return dsturl
}

// prepareDownloadDestination will return a new destination URL for
// remote->local copy operations.
func prepareLocalDestination(
	ctx context.Context,
	srcurl *url.URL,
	dsturl *url.URL,
	flatten bool,
	isBatch bool,
) (*url.URL, error) {
	objname := srcurl.Base()
	if isBatch && !flatten {
		objname = srcurl.Relative()
	}

	if isBatch {
		if err := os.MkdirAll(dsturl.Absolute(), os.ModePerm); err != nil {
			return nil, err
		}
	}

	client := storage.NewClient(dsturl)

	obj, err := client.Stat(ctx, dsturl)
	if err != nil && err != storage.ErrGivenObjectNotFound {
		return nil, err
	}

	if isBatch && !flatten {
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

// getObject checks if the object from given url exists. If no object is
// found, error and returning object would be nil.
func getObject(ctx context.Context, url *url.URL) (*storage.Object, error) {
	client := storage.NewClient(url)

	obj, err := client.Stat(ctx, url)
	if err == storage.ErrGivenObjectNotFound {
		return nil, nil
	}

	return obj, err
}

func validate(c *cli.Context) error {
	if c.Args().Len() != 2 {
		return fmt.Errorf("expected source and destination arguments")
	}

	ctx := c.Context
	src := c.Args().Get(0)
	dst := c.Args().Get(1)

	srcurl, err := url.New(src)
	if err != nil {
		return err
	}

	dsturl, err := url.New(dst)
	if err != nil {
		return err
	}

	// wildcard destination doesn't mean anything
	if dsturl.HasGlob() {
		return fmt.Errorf("target %q can not contain glob characters", dst)
	}

	// we don't operate on S3 prefixes for copy and delete operations.
	if srcurl.IsBucket() || srcurl.IsPrefix() {
		return fmt.Errorf("source argument must contain wildcard character")
	}

	// 'cp dir/* s3://bucket/prefix': expect a trailing slash to avoid any
	// surprises.
	if srcurl.HasGlob() && dsturl.IsRemote() && !dsturl.IsPrefix() && !dsturl.IsBucket() {
		return fmt.Errorf("target %q must be a bucket or a prefix", dsturl)
	}

	switch {
	case srcurl.Type == dsturl.Type:
		return validateCopy(srcurl, dsturl)
	case dsturl.IsRemote():
		return validateUpload(ctx, srcurl, dsturl)
	default:
		return nil
	}
}

func validateCopy(srcurl, dsturl *url.URL) error {
	if srcurl.IsRemote() || dsturl.IsRemote() {
		return nil
	}

	// we don't support local->local copies
	return fmt.Errorf("local->local copy operations are not permitted")
}

func validateUpload(ctx context.Context, srcurl, dsturl *url.URL) error {
	srcclient := storage.NewClient(srcurl)

	if srcurl.HasGlob() {
		return nil
	}

	obj, err := srcclient.Stat(ctx, srcurl)
	if err != nil {
		return err
	}

	// 'cp dir/ s3://bucket/prefix-without-slash': expect a trailing slash to
	// avoid any surprises.
	if obj.Type.IsDir() && !dsturl.IsBucket() && !dsturl.IsPrefix() {
		return fmt.Errorf("target %q must be a bucket or a prefix", dsturl)
	}

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
