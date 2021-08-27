package command

import (
	"context"
	"fmt"
	"os"
	"sort"
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
	"github.com/peak/s5cmd/utils"
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

			return NewSync(c).Run(c.Context)
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
func (s Sync) Run(ctx context.Context) error {
	srcurl, err := url.New(s.src)
	if err != nil {
		return err
	}

	dsturl, err := url.New(s.dst)
	if err != nil {
		return err
	}
	sourceClient, err := storage.NewClient(ctx, srcurl, s.storageOpts)
	if err != nil {
		return err
	}
	isBatch := srcurl.IsWildcard()

	if !isBatch && !srcurl.IsRemote() {
		obj, _ := sourceClient.Stat(ctx, srcurl)
		isBatch = obj != nil && obj.Type.IsDir()
	}

	sourceObjects, destObjects, err := s.GetSourceAndDestinationObjects(ctx, srcurl, dsturl)
	if err != nil {
		printError(s.fullCommand, s.op, err)
		return err
	}
	onlySource, onlyDest, commonObjects := CompareObjects(sourceObjects, destObjects)

	// clear the arrays.
	sourceObjects = nil
	destObjects = nil
	/* 	fmt.Printf("There are %d objects in only source\n", len(onlySource))
	   	fmt.Printf("There are %d objects in only des\n", len(onlyDest))
	   	fmt.Printf("There are %d objects in common\n", len(commonObjects)) */

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
	/* s.PlanRun(onlySource, onlyDest, commonObjects, dsturl, isBatch, strategy) */
	strategy := s.ChooseStrategy()
	tasks := s.Plan(ctx, onlySource, onlyDest, commonObjects, dsturl, isBatch, strategy)
	s.Execute(tasks, waiter)

	waiter.Wait()
	<-errDoneCh
	return merrorWaiter
}

func CompareObjects(sourceObjects, destObjects []*storage.Object) (srcOnly, dstOnly []*storage.Object, commonObj []*CommonObject) {
	// sort the source and destination objects.
	sort.Sort(utils.SortedObjectSlice(sourceObjects))
	sort.Sort(utils.SortedObjectSlice(destObjects))

	for iSrc, iDst := 0, 0; ; iSrc, iDst = iSrc+1, iDst+1 {
		var srcObject, dstObject *storage.Object
		var srcName, dstName string

		if iSrc < len(sourceObjects) {
			srcObject = sourceObjects[iSrc]
			srcName = srcObject.URL.ObjectPath()
		}

		if iDst < len(destObjects) {
			dstObject = destObjects[iDst]
			dstName = dstObject.URL.ObjectPath()
		}

		if srcObject == nil && dstObject == nil {
			break
		}

		if srcObject != nil && dstObject != nil {
			if srcName > dstName {
				srcObject = nil
				iSrc--
			} else if srcName == dstName {
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
			dstOnly = append(dstOnly, dstObject)
		case dstObject == nil:
			srcOnly = append(srcOnly, srcObject)
			/* default:
			fmt.Println("default case")
			commonObj = append(commonObj, &CommonObject{src: srcObject, dst: dstObject}) */

		}
	}
	return
}

func (s Sync) GetSourceAndDestinationObjects(ctx context.Context, srcurl, dsturl *url.URL) ([]*storage.Object, []*storage.Object, error) {
	var wg sync.WaitGroup
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
	// var wg sync.WaitGroup

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

func (s Sync) PlanRun(onlySource, onlyDest []*storage.Object, common []*CommonObject,
	dsturl *url.URL, isBatch bool, strategy utils.Strategy) {
	/* var tasks []parallel.Task */
	for _, srcobj := range onlySource {
		srcurl := srcobj.URL
		dsturlobj := prepareRemoteDestination(srcurl, dsturl, false, isBatch)
		fmt.Printf("cp %v %v\n", srcurl, dsturlobj)
	}

	for _, dstobj := range onlyDest {
		if s.shouldSkipObject(dstobj, true) {
			continue
		}
		fmt.Printf("delete %v\n", dstobj.URL)
	}

	for _, c := range common {
		if s.shouldSkipObject(c.src, true) {
			continue
		}
		if s.shouldSkipObject(c.dst, false) {
			continue
		}
		srcobj, dstobj := c.src, c.dst
		err := strategy.Compare(srcobj, dstobj)
		fmt.Printf("%v %v: %v\n", srcobj.URL, dstobj.URL, err)
		if err != nil {
			if errorpkg.IsWarning(err) {
				printDebug(s.op, srcobj.URL, dstobj.URL, err)
			}
			continue
		} else {
			fmt.Printf("cp %v %v\n", srcobj.URL, dstobj.URL)
		}
	}
}

func (s Sync) Plan(ctx context.Context, onlySource, onlyDest []*storage.Object, common []*CommonObject,
	dsturl *url.URL, isBatch bool, strategy utils.Strategy) []parallel.Task {
	tasks := make([]parallel.Task, 0)

	// only source objects.
	for _, srcobj := range onlySource {
		var task parallel.Task
		srcurl := srcobj.URL
		switch {
		case !srcurl.IsRemote() && dsturl.IsRemote(): // local->remote
			task = s.prepareUploadTask(ctx, srcurl, dsturl, isBatch)
		case srcurl.IsRemote() && !dsturl.IsRemote(): // remote->local
			task = s.prepareDownloadTask(ctx, srcurl, dsturl, isBatch)
		case srcurl.IsRemote() && dsturl.IsRemote(): // remote->remote
			task = s.prepareCopyTask(ctx, srcurl, dsturl, isBatch)
		default:
			panic("unexpected src-dst pair")
		}
		tasks = append(tasks, task)
	}

	for _, commonObject := range common {
		var task parallel.Task
		sourceObject, destObject := commonObject.src, commonObject.dst
		srcurl_local, dsturl_local := sourceObject.URL, destObject.URL
		err := strategy.Compare(sourceObject, destObject)
		if err != nil {
			if errorpkg.IsWarning(err) {
				printDebug(s.op, srcurl_local, dsturl_local, err)
				continue
			}
		}

		switch {
		case !srcurl_local.IsRemote() && destObject.URL.IsRemote(): // local->remote
			task = s.prepareUploadTask(ctx, srcurl_local, dsturl, isBatch)
		case srcurl_local.IsRemote() && !destObject.URL.IsRemote(): // remote->local
			task = s.prepareDownloadTask(ctx, srcurl_local, dsturl, isBatch)
		case srcurl_local.IsRemote() && destObject.URL.IsRemote(): // remote->remote
			task = s.prepareCopyTask(ctx, srcurl_local, dsturl, isBatch)
		default:
			panic("unexpected src-dst pair")
		}
		tasks = append(tasks, task)
	}

	for _, destObj := range onlyDest {
		task := s.prepareDeleteTask(ctx, destObj.URL)
		tasks = append(tasks, task)
	}
	return tasks
}

func (s Sync) Execute(tasks []parallel.Task, waiter *parallel.Waiter) {
	for _, task := range tasks {
		parallel.Run(task, waiter)
	}
}

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

func (s Sync) ChooseStrategy() utils.Strategy {
	if s.sizeOnly {
		return &utils.SizeOnly{}
	} else {
		return &utils.SizeAndModification{}
	}
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

		err = destClient.Delete(ctx, dsturl)
		if err == nil {
			msg := log.InfoMessage{
				Operation: "delete",
				Source:    dsturl,
			}
			log.Info(msg)
		}
		return err

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

func (s Sync) prepareDownloadTask(
	ctx context.Context,
	srcurl *url.URL,
	dsturl *url.URL,
	isBatch bool,
) func() error {
	return func() error {
		dsturl_local, err := prepareLocalDestination(ctx, srcurl, dsturl, false, isBatch, s.storageOpts)
		if err != nil {
			return err
		}
		err = s.doDownload(ctx, srcurl, dsturl_local)
		return returnError(err, "download", srcurl, dsturl)
	}
}

func (s Sync) prepareUploadTask(
	ctx context.Context,
	srcurl *url.URL,
	dsturl *url.URL,
	isBatch bool,
) func() error {
	return func() error {
		dsturl_local := prepareRemoteDestination(srcurl, dsturl, false, isBatch)
		err := s.doUpload(ctx, srcurl, dsturl_local)
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
