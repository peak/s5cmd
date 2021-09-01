package command

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/urfave/cli/v2"

	errorpkg "github.com/peak/s5cmd/error"
	"github.com/peak/s5cmd/internal/sortedslice"
	"github.com/peak/s5cmd/internal/strategy"
	"github.com/peak/s5cmd/internal/transfer"
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
Examples:
	01. Sync local folder to s3 bucket
		> s5cmd {{.HelpName}} folder/ s3://bucket/

	02. Sync S3 bucket to local folder
		> s5cmd {{.HelpName}} s3://bucket/* folder/

	03. Sync S3 bucket objects under prefix to S3 bucket.
		> s5cmd {{.HelpName}} s3://sourcebucket/prefix/* s3://destbucket/

	04. Sync local folder to S3 but delete the files that S3 bucket has but local does not have.
		> s5cmd {{.HelpName}} --delete folder/ s3://bucket/

	05. Sync S3 bucket to local folder but use size as only comparison criteria.
		> s5cmd {{.HelpName}} --size-only s3://bucket/* folder/
		
	06. Sync S3 bucket to local folder but check the md5 values of files.
		> s5cmd {{.HelpName}} --checksum s3://bucket/* folder/
	
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

			return NewSync(c).Run(c)
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
}

// NewSync creates Sync from cli.Context
func NewSync(c *cli.Context) Sync {
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
func (s Sync) Run(c *cli.Context) error {
	srcurl, err := url.New(s.src)
	if err != nil {
		return err
	}

	dsturl, err := url.New(s.dst)
	if err != nil {
		return err
	}
	sourceClient, err := storage.NewClient(c.Context, srcurl, s.storageOpts)
	if err != nil {
		return err
	}

	isBatch := srcurl.IsWildcard()
	if !isBatch && !srcurl.IsRemote() {
		obj, _ := sourceClient.Stat(c.Context, srcurl)
		isBatch = obj != nil && obj.Type.IsDir()
	}

	sourceObjects, destObjects, err := s.GetSourceAndDestinationObjects(c.Context, srcurl, dsturl)
	if err != nil {
		printError(s.fullCommand, s.op, err)
		return err
	}
	onlySource, onlyDest, commonObjects := CompareObjects(sourceObjects, destObjects)

	// clear the arrays.
	sourceObjects = nil
	destObjects = nil

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

	strategy := strategy.New(s.sizeOnly)                                                             // create comparison strategy.
	transferManager := transfer.NewManager(s.storageOpts, false, isBatch, s.concurrency, s.partSize) // create transfer manager
	pipeReader, pipeWriter := io.Pipe()                                                              // create a reader, writer pipe to pass commands to run

	// Create commands in background.
	go func() {
		s.PlanRun(c.Context, onlySource, onlyDest, commonObjects, dsturl, *transferManager, strategy, pipeWriter)
	}()

	// pass the reader to run
	return NewRun(c, pipeReader).Run(c)
}

func CompareObjects(sourceObjects, destObjects []*storage.Object) (srcOnly, dstOnly []*url.URL, commonObj []*CommonObject) {
	// sort the source and destination objects.
	sort.Sort(sortedslice.Slice(sourceObjects))
	sort.Sort(sortedslice.Slice(destObjects))

	for iSrc, iDst := 0, 0; ; iSrc, iDst = iSrc+1, iDst+1 {
		var srcObject, dstObject *storage.Object
		var srcName, dstName string

		if iSrc < len(sourceObjects) {
			srcObject = sourceObjects[iSrc]
			srcName = filepath.ToSlash(srcObject.URL.ObjectPath())
		}

		if iDst < len(destObjects) {
			dstObject = destObjects[iDst]
			dstName = filepath.ToSlash(dstObject.URL.ObjectPath())
		}

		if srcObject == nil && dstObject == nil {
			break
		}

		if srcObject != nil && dstObject != nil {
			if srcName > dstName {
				srcObject = nil
				iSrc--
			} else if srcName == dstName { // if there is a match.
				commonObj = append(commonObj, &CommonObject{src: srcObject, dst: dstObject})
			} else {
				dstObject = nil
				iDst--
			}
		}

		switch {
		case srcObject == nil && dstObject == nil:
			// do nothing
		case srcObject == nil:
			dstOnly = append(dstOnly, dstObject.URL)
		case dstObject == nil:
			srcOnly = append(srcOnly, srcObject.URL)
		}
	}
	return
}

// GetSourceAndDestinationObjects returns source and destination objects from given urls.
func (s Sync) GetSourceAndDestinationObjects(ctx context.Context, srcurl, dsturl *url.URL) ([]*storage.Object, []*storage.Object, error) {
	sourceClient, err := storage.NewClient(ctx, srcurl, s.storageOpts)
	if err != nil {
		return nil, nil, err
	}

	destClient, err := storage.NewClient(ctx, dsturl, s.storageOpts)
	if err != nil {
		return nil, nil, err
	}

	var sourceObjects []*storage.Object
	var destObjects []*storage.Object
	var wg sync.WaitGroup

	// get source objects.
	wg.Add(1)
	go func() {
		defer wg.Done()
		srcObjectChannel := sourceClient.List(ctx, srcurl, false)
		for srcObject := range srcObjectChannel {
			if s.shouldSkipObject(srcObject, true) {
				continue
			}
			sourceObjects = append(sourceObjects, srcObject)
		}
	}()

	// add * to end of destination string, to get all objects recursively.
	var destinationURLPath string
	if strings.HasSuffix(s.dst, "/") {
		destinationURLPath = s.dst + "*"
	} else {
		destinationURLPath = s.dst + "/*"
	}

	destObjectsURL, err := url.New(destinationURLPath)
	if err != nil {
		return nil, nil, err
	}

	// get destination objects.
	wg.Add(1)
	go func() {
		defer wg.Done()
		destObjectsChannel := destClient.List(ctx, destObjectsURL, false)
		for destObject := range destObjectsChannel {
			if s.shouldSkipObject(destObject, false) {
				continue
			}
			destObjects = append(destObjects, destObject)
		}
	}()

	// wait until source and destination objects are fetched.
	wg.Wait()
	return sourceObjects, destObjects, nil
}

// PlanRun prepares the commands and passes it to reader for run command.
func (s Sync) PlanRun(ctx context.Context, onlySource, onlyDest []*url.URL, common []*CommonObject,
	dsturl *url.URL, transferManager transfer.Manager, strategy strategy.Strategy, w *io.PipeWriter) {
	// only source objects.
	for _, srcurl := range onlySource {
		var dsturl_ *url.URL
		switch {
		case !srcurl.IsRemote() && dsturl.IsRemote(): // local->remote
			dsturl_ = transferManager.PrepareRemoteDestination(srcurl, dsturl)
		case srcurl.IsRemote() && !dsturl.IsRemote(): // remote->local
			dsturl_, _ = transferManager.PrepareLocalDestination(ctx, srcurl, dsturl)
		case srcurl.IsRemote() && dsturl.IsRemote(): // remote->remote
			dsturl_ = transferManager.PrepareRemoteDestination(srcurl, dsturl)
		default:
			panic("unexpected src-dst pair")
		}
		command := fmt.Sprintf("cp %v %v\n", srcurl, dsturl_)
		fmt.Fprint(w, command)
	}

	// for common objects
	for _, commonObject := range common {
		sourceObject, destObject := commonObject.src, commonObject.dst
		srcurl_local, dsturl_local := sourceObject.URL, destObject.URL
		err := strategy.Compare(sourceObject, destObject) // check if object should be copied.
		if err != nil {
			if errorpkg.IsWarning(err) {
				printDebug(s.op, srcurl_local, dsturl_local, err)
				continue
			}
		}

		command := fmt.Sprintf("cp %v %v\n", srcurl_local, dsturl_local)
		fmt.Fprint(w, command)
	}

	// for only destination objects.
	for _, desturl := range onlyDest {
		if s.delete { // if delete is set
			command := fmt.Sprintf("rm %v\n", desturl)
			fmt.Fprint(w, command)
		}
	}
	w.Close()

}

// shouldSkipObject checks is object should be skipped.
func (s Sync) shouldSkipObject(object *storage.Object, verbose bool) bool {
	if object.Type.IsDir() || errorpkg.IsCancelation(object.Err) {
		return true
	}

	if err := object.Err; err != nil {
		if verbose {
			printError(s.fullCommand, s.op, err)
		}
		return true
	}

	if object.StorageClass.IsGlacier() {
		if verbose {
			err := fmt.Errorf("object '%v' is on Glacier storage", object)
			printError(s.fullCommand, s.op, err)
		}
		return true
	}
	return false
}

func validateSyncCommand(c *cli.Context) error {
	if c.Args().Len() != 2 {
		return fmt.Errorf("expected source and destination arguments")
	}

	if c.Bool("size-only") && c.Bool("checksum") {
		return fmt.Errorf("--size-only and --checksum flags cannot be used together")
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
