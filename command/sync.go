package command

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/urfave/cli/v2"

	errorpkg "github.com/peak/s5cmd/error"
	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/log/stat"
	"github.com/peak/s5cmd/parallel"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
)

var syncHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] source destination

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
`

func NewSyncCommandFlags() []cli.Flag {
	return []cli.Flag{
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
		&cli.BoolFlag{
			Name:  "delete",
			Usage: "delete objects in destionation but not in source",
		},
		&cli.BoolFlag{
			Name:  "size-only",
			Usage: "make size of object only criteria to decide whether an object should be synced",
		},
	}
}

func NewSyncCommand() *cli.Command {
	return &cli.Command{
		Name:               "sync",
		HelpName:           "sync",
		Usage:              "sync objects",
		Flags:              NewSyncCommandFlags(),
		CustomHelpTemplate: syncHelpTemplate,
		Before: func(c *cli.Context) error {
			err := validateSyncCommand(c)
			if err != nil {
				printError(givenCommand(c), c.Command.Name, err)
			}
			return err
		},
		Action: func(c *cli.Context) (err error) {
			defer stat.Collect(c.Command.FullName(), &err)()

			return NewSync(c, false).Run(c.Context)
		},
	}
}

type CommonObject struct {
	src, dst *storage.Object
}

// Sync holds sync operation flags and states.
type Sync struct {
	src         string
	dst         string
	op          string
	fullCommand string

	// flags
	delete   bool
	sizeOnly bool

	// s3 options
	concurrency int
	partSize    int64
	storageOpts storage.Options

	// all objects
	sourceObjects []*storage.Object
	destObjects   []*storage.Object

	// object channels
	onlySource chan *storage.Object
	onlyDest   chan *url.URL
	commonObj  chan *CommonObject
}

// NewSync creates Sync from cli.Context
func NewSync(c *cli.Context, deleteSource bool) Sync {
	return Sync{
		src:         c.Args().Get(0),
		dst:         c.Args().Get(1),
		op:          c.Command.Name,
		fullCommand: givenCommand(c),

		// flags
		delete:   c.Bool("delete"),
		sizeOnly: c.Bool("size-only"),

		// s3 options
		partSize:    c.Int64("part-size") * megabytes,
		concurrency: c.Int("concurrency"),
		storageOpts: NewStorageOpts(c),
	}
}

// Run starts copying given source objects to destination.
func (s Sync) Run(ctx context.Context) error {
	srcurl, err := url.New(s.src)
	if err != nil {
		printError(s.fullCommand, s.op, err)
		return err
	}

	dsturl, err := url.New(s.dst)
	if err != nil {
		printError(s.fullCommand, s.op, err)
		return err
	}

	sourceClient, err := storage.NewClient(ctx, srcurl, s.storageOpts)
	if err != nil {
		printError(s.fullCommand, s.op, err)
		return err
	}

	destClient, err := storage.NewClient(ctx, dsturl, s.storageOpts)
	if err != nil {
		printError(s.fullCommand, s.op, err)
		return err
	}

	var wg sync.WaitGroup

	// get source objects.
	wg.Add(1)
	go func() {
		defer wg.Done()
		srcObjectChannel := sourceClient.List(ctx, srcurl, false)
		for srcObject := range srcObjectChannel {
			s.sourceObjects = append(s.sourceObjects, srcObject)
		}
	}()

	var destinationURLPath string
	if strings.HasSuffix(s.dst, "/") {
		destinationURLPath = s.dst + "*"
	} else {
		destinationURLPath = s.dst + "/*"
	}

	destObjectsURL, err := url.New(destinationURLPath)
	if err != nil {
		printError(s.fullCommand, s.op, err)
		return err
	}

	// get destination objects.
	wg.Add(1)
	go func() {
		defer wg.Done()
		destObjectsChannel := destClient.List(ctx, destObjectsURL, false)
		for destObject := range destObjectsChannel {
			s.destObjects = append(s.destObjects, destObject)
		}
	}()

	// wait until source and destination objects are fetched.
	wg.Wait()

	isBatch := srcurl.IsWildcard()
	if !isBatch && !srcurl.IsRemote() {
		obj, _ := sourceClient.Stat(ctx, srcurl)
		isBatch = obj != nil && obj.Type.IsDir()
	}

	s.commonObj = make(chan *CommonObject, len(s.sourceObjects))
	s.onlySource = make(chan *storage.Object, len(s.sourceObjects))
	s.onlyDest = make(chan *url.URL, len(s.destObjects))

	var (
		merrorChannelDestAndCommon error // used for creating destination and common objects channels.
		merrorChannelSource        error // used for creating only source object channel.
	)

	// detect only destination and common objects in background.
	go func() {
		for _, destObject := range s.destObjects {
			if s.shouldSkipObject(destObject, &merrorChannelDestAndCommon, true) {
				continue
			}
			foundIdx := s.doesSourceHave(s.sourceObjects, destObject, merrorChannelDestAndCommon)
			if foundIdx == -1 {
				s.onlyDest <- destObject.URL
			} else {
				s.commonObj <- &CommonObject{src: s.sourceObjects[foundIdx], dst: destObject}
			}
		}
		close(s.onlyDest)
		close(s.commonObj)

	}()

	// detect only source objects.
	go func() {
		for _, srcObject := range s.sourceObjects {
			if s.shouldSkipObject(srcObject, &merrorChannelSource, true) {
				continue
			}

			foundIdx := s.doesSourceHave(s.destObjects, srcObject, merrorChannelSource)
			if foundIdx == -1 {
				s.onlySource <- srcObject
			}
		}
		close(s.onlySource)
	}()

	waiter := parallel.NewWaiter()

	var (
		merrorWaiter error
		errDoneCh    = make(chan bool)
	)

	go func() {
		defer close(errDoneCh)
		for err := range waiter.Err() {
			if strings.Contains(err.Error(), "too many open files") {
				fmt.Println(strings.TrimSpace(fdlimitWarning))
				fmt.Printf("ERROR %v\n", err)

				os.Exit(1)
			}
			printError(s.fullCommand, s.op, err)
			merrorWaiter = multierror.Append(merrorWaiter, err)
		}
	}()

	// For the only source objects
	for sourceObject := range s.onlySource {
		var task parallel.Task
		srcurl := sourceObject.URL

		switch {
		case !sourceObject.URL.IsRemote() && dsturl.IsRemote(): // local->remote
			task = s.prepareUploadTask(ctx, srcurl, dsturl, isBatch)
		case sourceObject.URL.IsRemote() && !dsturl.IsRemote(): // remote->local
			task = s.prepareDownloadTask(ctx, srcurl, dsturl, isBatch)
		case sourceObject.URL.IsRemote() && dsturl.IsRemote(): // remote->remote
			task = s.prepareCopyTask(ctx, srcurl, dsturl, isBatch)
		default:
			panic("unexpected src-dst pair")
		}
		parallel.Run(task, waiter)
	}

	// for objects in both source and destination.
	for commonObject := range s.commonObj {
		var task parallel.Task
		sourceObject, destObject := commonObject.src, commonObject.dst

		switch {
		case !sourceObject.URL.IsRemote() && destObject.URL.IsRemote(): // local->remote
			task = s.commonUploadTask(ctx, sourceObject, destObject)
		case sourceObject.URL.IsRemote() && !destObject.URL.IsRemote(): // remote->local
			task = s.commonDownloadTask(ctx, sourceObject, destObject)
		case sourceObject.URL.IsRemote() && destObject.URL.IsRemote(): // remote->remote
			task = s.commonCopyTask(ctx, sourceObject, destObject)
		default:
			panic("unexpected src-dst pair")
		}
		parallel.Run(task, waiter)
	}

	// for delete objects in only destination.
	parallel.Run(s.prepareDeleteTask(ctx, dsturl), waiter)

	waiter.Wait()
	<-errDoneCh

	return multierror.Append(merrorChannelDestAndCommon, merrorWaiter, merrorChannelSource).ErrorOrNil()
}

func (s Sync) doesSourceHave(sourceObjects []*storage.Object, wantedObject *storage.Object, errorToWrite error) int {
	for idx, source := range sourceObjects {
		if s.shouldSkipObject(source, &errorToWrite, false) {
			continue
		}
		if filepath.ToSlash(source.URL.ObjectPath()) == filepath.ToSlash(wantedObject.URL.ObjectPath()) {
			return idx
		}
	}
	return -1
}

func (s Sync) shouldSkipObject(object *storage.Object, errorToWrite *error, verbose bool) bool {
	if object.Type.IsDir() || errorpkg.IsCancelation(object.Err) {
		return true
	}

	if err := object.Err; err != nil {
		return true
	}

	if object.StorageClass.IsGlacier() {
		err := fmt.Errorf("object '%v' is on Glacier storage", object)
		*errorToWrite = multierror.Append(*errorToWrite, err)
		if verbose {
			printError(s.fullCommand, s.op, err)
		}
		return true
	}
	return false
}

// prepareDeleteTask prepares delete operation of only destination objects.
func (s Sync) prepareDeleteTask(
	ctx context.Context,
	dsturl *url.URL,
) func() error {
	return func() error {
		// if delete is not set, then return.
		if !s.delete {
			return nil
		}
		destClient, err := storage.NewClient(ctx, dsturl, s.storageOpts)
		if err != nil {
			return err
		}

		var merrorDelete error
		resultch := destClient.MultiDelete(ctx, s.onlyDest)
		for obj := range resultch {
			if err := obj.Err; err != nil {
				if errorpkg.IsCancelation(obj.Err) {
					continue
				}
				merrorDelete = multierror.Append(merrorDelete, obj.Err)
				printError(s.fullCommand, s.op, obj.Err)
				continue
			}

			msg := log.InfoMessage{
				Operation: "delete",
				Source:    obj.URL,
			}
			log.Info(msg)
		}
		return nil
	}
}

// commonCopyTask prepares copy operation (remote->remote) of objects both in source and destination.
func (s Sync) commonCopyTask(
	ctx context.Context,
	srcObj *storage.Object,
	dstObj *storage.Object,
) func() error {
	return func() error {
		srcurl, dsturl := srcObj.URL, dstObj.URL
		err := s.shouldOverride(srcObj, dstObj) // check if source object should be overridden
		if err != nil {
			if errorpkg.IsWarning(err) {
				printDebug(s.op, srcurl, dsturl, err)
				return nil
			}
			return err
		}
		err = s.doCopy(ctx, srcurl, dsturl)
		return returnError(err, "copy", srcurl, dsturl)
	}
}

func (s Sync) prepareCopyTask(
	ctx context.Context,
	srcurl *url.URL,
	dsturl *url.URL,
	isBatch bool,
) func() error {
	return func() error {
		dsturl = prepareRemoteDestination(srcurl, dsturl, false, isBatch)
		err := s.doCopy(ctx, srcurl, dsturl)
		return returnError(err, "copy", srcurl, dsturl)
	}
}

// commonDownloadTask prepares download operation (remote->local) of objects both in source and destination.
func (s Sync) commonDownloadTask(
	ctx context.Context,
	srcObj *storage.Object,
	dstObj *storage.Object,
) func() error {
	return func() error {
		err := s.shouldOverride(srcObj, dstObj) // check if source object should be overridden
		srcurl, dsturl := srcObj.URL, dstObj.URL
		if err != nil {
			if errorpkg.IsWarning(err) {
				printDebug(s.op, srcurl, dsturl, err)
				return nil
			}
			return err
		}
		err = s.doDownload(ctx, srcurl, dsturl)
		return returnError(err, "download", srcurl, dsturl)
	}
}

func (s Sync) prepareDownloadTask(
	ctx context.Context,
	srcurl *url.URL,
	dsturl *url.URL,
	isBatch bool,
) func() error {
	return func() error {
		dsturl, err := prepareLocalDestination(ctx, srcurl, dsturl, false, isBatch, s.storageOpts)
		if err != nil {
			return err
		}
		err = s.doDownload(ctx, srcurl, dsturl)
		return returnError(err, "download", srcurl, dsturl)
	}
}

// commonUploadTask prepares upload operation (local->remote) of objects both in source and destination.
func (s Sync) commonUploadTask(
	ctx context.Context,
	srcObj *storage.Object,
	dstObj *storage.Object,
) func() error {
	return func() error {
		err := s.shouldOverride(srcObj, dstObj) // check if source object should be overridden
		srcurl, dsturl := srcObj.URL, dstObj.URL
		if err != nil {
			if errorpkg.IsWarning(err) {
				printDebug(s.op, srcurl, dsturl, err)
				return nil
			}
			return err
		}
		err = s.doUpload(ctx, srcurl, dsturl)
		return returnError(err, "upload", srcurl, dsturl)
	}
}

func (s Sync) prepareUploadTask(
	ctx context.Context,
	srcurl *url.URL,
	dsturl *url.URL,
	isBatch bool,
) func() error {
	return func() error {
		dsturl = prepareRemoteDestination(srcurl, dsturl, false, isBatch)
		err := s.doUpload(ctx, srcurl, dsturl)
		return returnError(err, "upload", srcurl, dsturl)
	}
}

// doDownload is used to fetch a remote object and save as a local object.
func (s Sync) doDownload(ctx context.Context, srcurl *url.URL, dsturl *url.URL) error {
	srcClient, err := storage.NewRemoteClient(ctx, srcurl, s.storageOpts)
	if err != nil {
		return err
	}

	dstClient := storage.NewLocalClient(s.storageOpts)

	file, err := dstClient.Create(dsturl.Absolute())
	if err != nil {
		return err
	}
	defer file.Close()

	size, err := srcClient.Get(ctx, srcurl, file, s.concurrency, s.partSize)
	if err != nil {
		_ = dstClient.Delete(ctx, dsturl)
		return err
	}

	msg := log.InfoMessage{
		Operation:   "download",
		Source:      srcurl,
		Destination: dsturl,
		Object: &storage.Object{
			Size: size,
		},
	}
	log.Info(msg)

	return nil
}

func (s Sync) doUpload(ctx context.Context, srcurl *url.URL, dsturl *url.URL) error {
	srcClient := storage.NewLocalClient(s.storageOpts)

	file, err := srcClient.Open(srcurl.Absolute())
	if err != nil {
		return err
	}
	defer file.Close()

	dstClient, err := storage.NewRemoteClient(ctx, dsturl, s.storageOpts)
	if err != nil {
		return err
	}

	metadata := storage.NewMetadata()

	err = dstClient.Put(ctx, file, dsturl, metadata, s.concurrency, s.partSize)
	if err != nil {
		return err
	}

	obj, _ := srcClient.Stat(ctx, srcurl)
	size := obj.Size

	msg := log.InfoMessage{
		Operation:   "upload",
		Source:      srcurl,
		Destination: dsturl,
		Object: &storage.Object{
			Size: size,
		},
	}
	log.Info(msg)

	return nil
}

func (s Sync) doCopy(ctx context.Context, srcurl, dsturl *url.URL) error {

	dstClient, err := storage.NewClient(ctx, dsturl, s.storageOpts)
	if err != nil {
		return err
	}

	metadata := storage.NewMetadata()

	err = dstClient.Copy(ctx, srcurl, dsturl, metadata)
	if err != nil {
		return err
	}

	msg := log.InfoMessage{
		Operation:   "copy",
		Source:      srcurl,
		Destination: dsturl,
		Object: &storage.Object{
			URL: dsturl,
		},
	}
	log.Info(msg)

	return nil
}

// shouldOverride function checks if the destination should be overridden.
// it checks object sizes and modification times and if any of those things
// are different, then it returns an error which says this objects should not
// be overridden. If it returns nil, then it means it means override operation
// needs to be done.
func (s Sync) shouldOverride(srcObj *storage.Object, dstObj *storage.Object) error {
	var stickyErr error
	// check size of objects
	/* 	fmt.Printf("size for %s: %d, %s: %d\n", srcObj.URL.Path, srcObj.Size, dstObj.URL.Path, dstObj.Size)
	   	fmt.Printf("etags :%s : %s, %s : %s\n", srcObj.URL.Path, srcObj.Etag, dstObj.URL.Path, dstObj.Etag) */
	if srcObj.Size == dstObj.Size {
		stickyErr = errorpkg.ErrObjectSizesMatch
	} else {
		stickyErr = nil
	}

	srcMod, dstMod := srcObj.ModTime, dstObj.ModTime
	// if size only flag is set, then do not check the time
	if !s.sizeOnly {
		if !srcMod.After(*dstMod) {
			stickyErr = errorpkg.ErrObjectIsNewer
		} else {
			stickyErr = nil
		}
	}

	return stickyErr
}

func validateSyncCommand(c *cli.Context) error {
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
	if dsturl.IsWildcard() {
		return fmt.Errorf("target %q can not contain glob characters", dst)
	}

	// we don't operate on S3 prefixes for copy and delete operations.
	if srcurl.IsBucket() || srcurl.IsPrefix() {
		return fmt.Errorf("source argument must contain wildcard character")
	}

	// 'cp dir/* s3://bucket/prefix': expect a trailing slash to avoid any
	// surprises.
	if srcurl.IsWildcard() && dsturl.IsRemote() && !dsturl.IsPrefix() && !dsturl.IsBucket() {
		return fmt.Errorf("target %q must be a bucket or a prefix", dsturl)
	}

	switch {
	case srcurl.Type == dsturl.Type:
		return validateSyncCopy(srcurl, dsturl)
	case dsturl.IsRemote():
		return validateSyncUpload(ctx, srcurl, dsturl, NewStorageOpts(c))
	case srcurl.IsRemote():
		return validateSyncDownload(srcurl)
	default:
		return nil
	}
}

func validateSyncCopy(srcurl, dsturl *url.URL) error {
	if srcurl.IsRemote() || dsturl.IsRemote() {
		return nil
	}

	// we don't support local->local copies
	return fmt.Errorf("local->local sync operations are not permitted")
}

func validateSyncUpload(ctx context.Context, srcurl, dsturl *url.URL, storageOpts storage.Options) error {
	srcclient := storage.NewLocalClient(storageOpts)

	if srcurl.IsWildcard() {
		return nil
	}

	obj, err := srcclient.Stat(ctx, srcurl)
	if err != nil {
		return err
	}

	// do not support single file. use 'cp' instead.
	if !obj.Type.IsDir() {
		return fmt.Errorf("local source must be a directory")
	}

	// 'sync dir/ s3://bucket/prefix-without-slash': expect a trailing slash to
	// avoid any surprises.
	if obj.Type.IsDir() && !dsturl.IsBucket() && !dsturl.IsPrefix() {
		return fmt.Errorf("target %q must be a bucket or a prefix", dsturl)
	}

	return nil
}

func validateSyncDownload(srcurl *url.URL) error {
	if srcurl.IsWildcard() {
		return nil
	}

	// 'sync s3://bucket/prefix-without-slash dir/': should not work
	// 'sync s3://bucket/object.go dir/' should not work.
	// do not support single object.
	if !srcurl.IsBucket() && !srcurl.IsPrefix() {
		return fmt.Errorf("remote source %q must be a bucket or a prefix", srcurl)
	}

	return nil
}

func returnError(err error, op string, srcurl, dsturl *url.URL) error {
	if err != nil {
		return &errorpkg.Error{
			Op:  op,
			Src: srcurl,
			Dst: dsturl,
			Err: err,
		}
	}
	return nil
}
