package command

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/hashicorp/go-multierror"
	"github.com/lanrat/extsort"
	"github.com/urfave/cli/v2"

	errorpkg "github.com/peak/s5cmd/v2/error"
	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/log/stat"
	"github.com/peak/s5cmd/v2/parallel"
	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
)

const (
	extsortChannelBufferSize = 1_000
	extsortChunkSize         = 100_000
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
		 > s5cmd {{.HelpName}} "s3://bucket/*" folder/

	03. Sync S3 bucket objects under prefix to S3 bucket.
		 > s5cmd {{.HelpName}} "s3://sourcebucket/prefix/*" s3://destbucket/

	04. Sync local folder to S3 but delete the files that S3 bucket has but local does not have.
		 > s5cmd {{.HelpName}} --delete folder/ s3://bucket/

	05. Sync S3 bucket to local folder but use size as only comparison criteria.
		 > s5cmd {{.HelpName}} --size-only "s3://bucket/*" folder/

	06. Sync S3 bucket to local folder but use size and hash as comparasion criteria.
		 > s5cmd {{.HelpName}} --hash-only "s3://bucket/*" folder/

	07. Sync a file to S3 bucket
		 > s5cmd {{.HelpName}} myfile.gz s3://bucket/

	08. Sync matching S3 objects to another bucket
		 > s5cmd {{.HelpName}} "s3://bucket/*.gz" s3://target-bucket/prefix/

	09. Perform KMS Server Side Encryption of the object(s) at the destination
		 > s5cmd {{.HelpName}} --sse aws:kms s3://bucket/object s3://target-bucket/prefix/object

	10. Perform KMS-SSE of the object(s) at the destination using customer managed Customer Master Key (CMK) key id
		 > s5cmd {{.HelpName}} --sse aws:kms --sse-kms-key-id <your-kms-key-id> s3://bucket/object s3://target-bucket/prefix/object

	11. Sync all files to S3 bucket but exclude the ones with txt and gz extension
		 > s5cmd {{.HelpName}} --exclude "*.txt" --exclude "*.gz" dir/ s3://bucket

	12. Sync all files to S3 bucket but include the only ones with txt and gz extension
		 > s5cmd {{.HelpName}} --include "*.txt" --include "*.gz" dir/ s3://bucket
`

func NewSyncCommandFlags() []cli.Flag {
	syncFlags := []cli.Flag{
		&cli.BoolFlag{
			Name:  "delete",
			Usage: "delete objects in destination but not in source",
		},
		&cli.BoolFlag{
			Name:  "size-only",
			Usage: "make size of object only criteria to decide whether an object should be synced",
		},
		&cli.BoolFlag{
			Name:  "hash-only",
			Usage: "make hash and size of object only criteria to decide whether an object should be synced",
		},
		&cli.BoolFlag{
			Name:  "exit-on-error",
			Usage: "stops the sync process if an error is received",
		},
	}
	sharedFlags := NewSharedFlags()
	return append(syncFlags, sharedFlags...)
}

func NewSyncCommand() *cli.Command {
	cmd := &cli.Command{
		Name:               "sync",
		HelpName:           "sync",
		Usage:              "sync objects",
		Flags:              NewSyncCommandFlags(),
		CustomHelpTemplate: syncHelpTemplate,
		Before: func(c *cli.Context) error {
			// sync command share same validation method as copy command
			err := validateCopyCommand(c)
			if err != nil {
				printError(commandFromContext(c), c.Command.Name, err)
			}
			return err
		},
		Action: func(c *cli.Context) (err error) {
			defer stat.Collect(c.Command.FullName(), &err)()

			return NewSync(c).Run(c)
		},
	}

	cmd.BashComplete = getBashCompleteFn(cmd, false, false)
	return cmd
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
	delete      bool
	sizeOnly    bool
	hashOnly    bool
	exitOnError bool

	// s3 options
	storageOpts storage.Options

	followSymlinks bool
	storageClass   storage.StorageClass
	raw            bool
	numWorkers     int

	srcRegion string
	dstRegion string
}

// NewSync creates Sync from cli.Context
func NewSync(c *cli.Context) Sync {
	return Sync{
		src:         c.Args().Get(0),
		dst:         c.Args().Get(1),
		op:          c.Command.Name,
		fullCommand: commandFromContext(c),

		// flags
		delete:      c.Bool("delete"),
		sizeOnly:    c.Bool("size-only"),
		hashOnly:    c.Bool("hash-only"),
		exitOnError: c.Bool("exit-on-error"),

		// flags
		followSymlinks: !c.Bool("no-follow-symlinks"),
		storageClass:   storage.StorageClass(c.String("storage-class")),
		raw:            c.Bool("raw"),
		numWorkers:     c.Int("numworkers"),
		// region settings
		srcRegion:   c.String("source-region"),
		dstRegion:   c.String("destination-region"),
		storageOpts: NewStorageOpts(c),
	}
}

// Run compares files, plans necessary s5cmd commands to execute
// and executes them in order to sync source to destination.
func (s Sync) Run(c *cli.Context) error {
	srcurl, err := url.New(s.src, url.WithRaw(s.raw))
	if err != nil {
		return err
	}

	dsturl, err := url.New(s.dst, url.WithRaw(s.raw))
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(c.Context)

	sourceObjects, destObjects, err := s.getSourceAndDestinationObjects(ctx, cancel, srcurl, dsturl)
	if err != nil {
		printError(s.fullCommand, s.op, err)
		return err
	}

	isBatch := srcurl.IsWildcard()
	if !isBatch && !srcurl.IsRemote() {
		sourceClient, err := storage.NewClient(ctx, srcurl, s.storageOpts)
		if err != nil {
			return err
		}

		obj, err := sourceClient.Stat(ctx, srcurl)
		if err != nil {
			return err
		}

		isBatch = obj != nil && obj.Type.IsDir()
	}

	onlySource, onlyDest, commonObjects := compareObjects(sourceObjects, destObjects, isBatch)

	sourceObjects = nil
	destObjects = nil

	waiter := parallel.NewWaiter()
	var (
		merrorWaiter error
		errDoneCh    = make(chan struct{})
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

	strategy := NewStrategy(s.sizeOnly, s.hashOnly) // create comparison strategy.
	pipeReader, pipeWriter := io.Pipe()             // create a reader, writer pipe to pass commands to run

	// Create commands in background.
	go s.planRun(c, onlySource, onlyDest, commonObjects, dsturl, strategy, pipeWriter, isBatch, s.numWorkers)

	err = NewRun(c, pipeReader).Run(ctx)
	return multierror.Append(err, merrorWaiter).ErrorOrNil()
}

// compareObjects compares source and destination objects. It assumes that
// sourceObjects and destObjects channels are already sorted in ascending order.
// Returns objects those in only source, only destination
// and both.
func compareObjects(sourceObjects, destObjects chan *storage.Object, isSrcBatch bool) (chan *url.URL, chan *url.URL, chan *ObjectPair) {
	var (
		srcOnly   = make(chan *url.URL, extsortChannelBufferSize)
		dstOnly   = make(chan *url.URL, extsortChannelBufferSize)
		commonObj = make(chan *ObjectPair, extsortChannelBufferSize)
		srcName   string
		dstName   string
	)

	go func() {
		src, srcOk := <-sourceObjects
		dst, dstOk := <-destObjects

		defer close(srcOnly)
		defer close(dstOnly)
		defer close(commonObj)

		for {
			if srcOk {
				srcName = filepath.ToSlash(src.URL.Relative())
				if !isSrcBatch {
					srcName = src.URL.Base()
				}
			}
			if dstOk {
				dstName = filepath.ToSlash(dst.URL.Relative())
			}

			if srcOk && dstOk {
				if srcName < dstName {
					srcOnly <- src.URL
					src, srcOk = <-sourceObjects
				} else if srcName == dstName { // if there is a match.
					commonObj <- &ObjectPair{src: src, dst: dst}
					src, srcOk = <-sourceObjects
					dst, dstOk = <-destObjects
				} else {
					dstOnly <- dst.URL
					dst, dstOk = <-destObjects
				}
			} else if srcOk {
				srcOnly <- src.URL
				src, srcOk = <-sourceObjects
			} else if dstOk {
				dstOnly <- dst.URL
				dst, dstOk = <-destObjects
			} else /* if !srcOK && !dstOk */ {
				break
			}
		}
	}()

	return srcOnly, dstOnly, commonObj
}

// getSourceAndDestinationObjects returns source and destination objects from
// given URLs. The returned channels gives objects sorted in ascending order
// with respect to their url.Relative path. See also storage.Less.
func (s Sync) getSourceAndDestinationObjects(ctx context.Context, cancel context.CancelFunc, srcurl, dsturl *url.URL) (chan *storage.Object, chan *storage.Object, error) {
	sourceClient, err := storage.NewClient(ctx, srcurl, s.storageOpts)
	if err != nil {
		return nil, nil, err
	}

	destClient, err := storage.NewClient(ctx, dsturl, s.storageOpts)
	if err != nil {
		return nil, nil, err
	}

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

	var (
		sourceObjects = make(chan *storage.Object, extsortChannelBufferSize)
		destObjects   = make(chan *storage.Object, extsortChannelBufferSize)
	)

	extsortDefaultConfig := extsort.DefaultConfig()
	extsortConfig := &extsort.Config{
		ChunkSize:          extsortChunkSize,
		NumWorkers:         extsortDefaultConfig.NumWorkers,
		ChanBuffSize:       extsortChannelBufferSize,
		SortedChanBuffSize: extsortChannelBufferSize,
	}
	extsortDefaultConfig = nil

	// get source objects.
	go func() {
		defer close(sourceObjects)
		unfilteredSrcObjectChannel := sourceClient.List(ctx, srcurl, s.followSymlinks)
		filteredSrcObjectChannel := make(chan extsort.SortType, extsortChannelBufferSize)

		go func() {
			defer close(filteredSrcObjectChannel)
			// filter and redirect objects
			for st := range unfilteredSrcObjectChannel {
				if st.Err != nil && s.shouldStopSync(st.Err) {
					msg := log.ErrorMessage{
						Err:       cleanupError(st.Err),
						Command:   s.fullCommand,
						Operation: s.op,
					}
					log.Error(msg)
					cancel()
				}
				if s.shouldSkipSrcObject(st, true) {
					continue
				}
				filteredSrcObjectChannel <- *st
			}
		}()

		var (
			sorter        *extsort.SortTypeSorter
			srcOutputChan chan extsort.SortType
		)

		sorter, srcOutputChan, srcErrCh := extsort.New(filteredSrcObjectChannel, storage.FromBytes, storage.Less, extsortConfig)
		sorter.Sort(ctx)

		for srcObject := range srcOutputChan {
			o := srcObject.(storage.Object)
			sourceObjects <- &o
		}

		// read and print the external sort errors
		go func() {
			for err := range srcErrCh {
				printError(s.fullCommand, s.op, err)
			}
		}()
	}()

	// get destination objects.
	go func() {
		defer close(destObjects)
		unfilteredDestObjectsChannel := destClient.List(ctx, destObjectsURL, false)
		filteredDstObjectChannel := make(chan extsort.SortType, extsortChannelBufferSize)

		go func() {
			defer close(filteredDstObjectChannel)
			// filter and redirect objects
			for dt := range unfilteredDestObjectsChannel {
				if dt.Err != nil && s.shouldStopSync(dt.Err) {
					msg := log.ErrorMessage{
						Err:       cleanupError(dt.Err),
						Command:   s.fullCommand,
						Operation: s.op,
					}
					log.Error(msg)
					cancel()
				}
				if s.shouldSkipDstObject(dt, false) {
					continue
				}
				filteredDstObjectChannel <- *dt
			}
		}()

		var (
			dstSorter     *extsort.SortTypeSorter
			dstOutputChan chan extsort.SortType
		)

		dstSorter, dstOutputChan, dstErrCh := extsort.New(filteredDstObjectChannel, storage.FromBytes, storage.Less, extsortConfig)
		dstSorter.Sort(ctx)

		for destObject := range dstOutputChan {
			o := destObject.(storage.Object)
			destObjects <- &o
		}

		// read and print the external sort errors
		go func() {
			for err := range dstErrCh {
				printError(s.fullCommand, s.op, err)
			}
		}()
	}()

	return sourceObjects, destObjects, nil
}

// planRun prepares the commands and writes them to writer 'w'.
func (s Sync) planRun(
	c *cli.Context,
	onlySource, onlyDest chan *url.URL,
	common chan *ObjectPair,
	dsturl *url.URL,
	strategy SyncStrategy,
	w io.WriteCloser,
	isBatch bool,
	numWorkers int,
) {
	defer w.Close()

	// Always use raw mode since sync command generates commands
	// from raw S3 objects. Otherwise, generated copy command will
	// try to expand given source.
	defaultFlags := map[string]interface{}{
		"raw": true,
	}

	// it should wait until both of the child goroutines for onlySource and common channels
	// are completed before closing the WriteCloser w to ensure that all URLs are processed.
	var wg sync.WaitGroup

	// only in source
	wg.Add(1)
	go func() {
		defer wg.Done()
		for srcurl := range onlySource {
			curDestURL := generateDestinationURL(srcurl, dsturl, isBatch)
			command, err := generateCommand(c, "cp", defaultFlags, srcurl, curDestURL)
			if err != nil {
				printDebug(s.op, err, srcurl, curDestURL)
				continue
			}
			fmt.Fprintln(w, command)
		}
	}()

	// both in source and destination
	// needs several goroutines because HashSync reads a lot of files from the file system
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for commonObject := range common {
				sourceObject, destObject := commonObject.src, commonObject.dst
				curSourceURL, curDestURL := sourceObject.URL, destObject.URL
				err := strategy.ShouldSync(sourceObject, destObject) // check if object should be copied.
				if err != nil {
					printDebug(s.op, err, curSourceURL, curDestURL)
					continue
				}

				command, err := generateCommand(c, "cp", defaultFlags, curSourceURL, curDestURL)
				if err != nil {
					printDebug(s.op, err, curSourceURL, curDestURL)
					continue
				}
				fmt.Fprintln(w, command)
			}
		}()
	}

	// only in destination
	wg.Add(1)
	go func() {
		defer wg.Done()
		if s.delete {
			// unfortunately we need to read them all!
			// or rewrite generateCommand function?
			dstURLs := make([]*url.URL, 0, extsortChunkSize)

			for d := range onlyDest {
				dstURLs = append(dstURLs, d)
			}

			if len(dstURLs) == 0 {
				return
			}

			command, err := generateCommand(c, "rm", defaultFlags, dstURLs...)
			if err != nil {
				printDebug(s.op, err, dstURLs...)
				return
			}
			fmt.Fprintln(w, command)
		} else {
			// we only need  to consume them from the channel so that rest of the objects
			// can be sent to channel.
			for d := range onlyDest {
				_ = d
			}
		}
	}()

	wg.Wait()
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
func (s Sync) shouldSkipSrcObject(object *storage.Object, verbose bool) bool {
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

func (s Sync) shouldSkipDstObject(object *storage.Object, verbose bool) bool {
	if object.Type.IsDir() || errorpkg.IsCancelation(object.Err) {
		return true
	}

	if err := object.Err; err != nil {
		if verbose {
			printError(s.fullCommand, s.op, err)
		}
		return true
	}

	return false
}

// shouldStopSync determines whether a sync process should be stopped or not.
func (s Sync) shouldStopSync(err error) bool {
	if err == storage.ErrNoObjectFound {
		return false
	}
	if awsErr, ok := err.(awserr.Error); ok {
		switch awsErr.Code() {
		case "AccessDenied", "NoSuchBucket":
			return true
		}
	}
	return s.exitOnError
}
