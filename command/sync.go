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
			Usage: "delete objects in destination but not in source",
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

type ObjectPair struct {
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

// Run compares files, plans necessary s5cmd commands to execute
// and executes them in order to sync source to destination.
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

	sourceObjects, destObjects, err := s.getSourceAndDestinationObjects(c.Context, srcurl, dsturl)
	if err != nil {
		printError(s.fullCommand, s.op, err)
		return err
	}
	onlySource, onlyDest, commonObjects := compareObjects(sourceObjects, destObjects)

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

	strategy := NewStrategy(s.sizeOnly) // create comparison strategy.
	pipeReader, pipeWriter := io.Pipe() // create a reader, writer pipe to pass commands to run

	// Create commands in background.
	go s.planRun(onlySource, onlyDest, commonObjects, dsturl, strategy, pipeWriter, isBatch)

	err = NewRun(c, pipeReader).Run(c.Context)
	return multierror.Append(err, merrorWaiter).ErrorOrNil()
}

// compareObjects compares source and destination objects.
// Returns objects those in only source, only destination
// and both.
// The algorithm is taken from;
// https://github.com/rclone/rclone/blob/HEAD/fs/march/march.go#L304
func compareObjects(sourceObjects, destObjects []*storage.Object) ([]*url.URL, []*url.URL, []*ObjectPair) {
	// sort the source and destination objects.
	sort.SliceStable(sourceObjects, func(i, j int) bool { return sourceObjects[i].URL.Relative() < sourceObjects[j].URL.Relative() })
	sort.SliceStable(destObjects, func(i, j int) bool { return destObjects[i].URL.Relative() < destObjects[j].URL.Relative() })

	var srcOnly []*url.URL
	var dstOnly []*url.URL
	var commonObj []*ObjectPair

	for iSrc, iDst := 0, 0; ; iSrc, iDst = iSrc+1, iDst+1 {
		var srcObject, dstObject *storage.Object
		var srcName, dstName string

		if iSrc < len(sourceObjects) {
			srcObject = sourceObjects[iSrc]
			srcName = filepath.ToSlash(srcObject.URL.Relative())
		}

		if iDst < len(destObjects) {
			dstObject = destObjects[iDst]
			dstName = filepath.ToSlash(dstObject.URL.Relative())
		}

		if srcObject == nil && dstObject == nil {
			break
		}

		if srcObject != nil && dstObject != nil {
			if srcName > dstName {
				srcObject = nil
				iSrc--
			} else if srcName == dstName { // if there is a match.
				commonObj = append(commonObj, &ObjectPair{src: srcObject, dst: dstObject})
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
	return srcOnly, dstOnly, commonObj
}

// getSourceAndDestinationObjects returns source and destination
// objects from given urls.
func (s Sync) getSourceAndDestinationObjects(ctx context.Context, srcurl, dsturl *url.URL) ([]*storage.Object, []*storage.Object, error) {
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

	wg.Wait()
	return sourceObjects, destObjects, nil
}

// planRun prepares the commands and writes them to writer 'w'.
func (s Sync) planRun(
	onlySource, onlyDest []*url.URL,
	common []*ObjectPair,
	dsturl *url.URL,
	strategy SyncStrategy,
	w io.WriteCloser,
	isBatch bool,
) {

	// only in source
	for _, srcurl := range onlySource {
		curDestURL := generateDestinationURL(srcurl, dsturl, isBatch)

		command := fmt.Sprintf("cp %v %v\n", srcurl, curDestURL)
		fmt.Fprint(w, command)
	}

	// both in source and destination
	for _, commonObject := range common {
		sourceObject, destObject := commonObject.src, commonObject.dst
		curSourceURL, curDestURL := sourceObject.URL, destObject.URL
		err := strategy.ShouldSync(sourceObject, destObject) // check if object should be copied.
		if err != nil {
			printDebug(s.op, curSourceURL, curDestURL, err)
			continue
		}

		command := fmt.Sprintf("cp %v %v\n", curSourceURL, curDestURL)
		fmt.Fprint(w, command)
	}

	// only in destination
	if s.delete && len(onlyDest) > 0 {
		var objectsToDelete []string
		for _, obj := range onlyDest {
			objectsToDelete = append(objectsToDelete, obj.String())
		}

		command := fmt.Sprintf("rm %v\n", strings.Join(objectsToDelete, " "))
		fmt.Fprint(w, command)
	}

	w.Close()
}

// generateDestinationURL generates destination url for given
// source url if it would have been in destination.
func generateDestinationURL(srcurl, dsturl *url.URL, isBatch bool) *url.URL {
	objname := srcurl.Base()
	if isBatch {
		objname = srcurl.Relative()
	}

	if dsturl.IsRemote() {
		if dsturl.IsPrefix() || dsturl.IsBucket() {
			return dsturl.Join(objname)
		}
		return dsturl.Clone()

	}

	return dsturl.Join(objname)
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
