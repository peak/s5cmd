package command

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/urfave/cli/v2"

	errorpkg "github.com/peak/s5cmd/v2/error"
	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/log/stat"
	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
	"github.com/peak/s5cmd/v2/strutil"
)

var listHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] argument

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. List all buckets
		 > s5cmd {{.HelpName}}

	2. List objects and prefixes in a bucket
		 > s5cmd {{.HelpName}} s3://bucket/

	3. List all objects in a bucket
		 > s5cmd {{.HelpName}} "s3://bucket/*"

	4. List all objects that matches a wildcard
		 > s5cmd {{.HelpName}} "s3://bucket/prefix/*/*.gz"

	5. List all objects in a public bucket
		 > s5cmd --no-sign-request {{.HelpName}} "s3://bucket/*"

	6. List all objects in a bucket but exclude the ones with prefix abc
		 > s5cmd {{.HelpName}} --exclude "abc*" "s3://bucket/*"

	7. List all object in a requester pays bucket
		 > s5cmd --request-payer=requester {{.HelpName}} "s3://bucket/*"

	8. List all versions of an object in the bucket
		 > s5cmd {{.HelpName}} --all-versions s3://bucket/object

	9. List all versions of all objects that starts with a prefix in the bucket
		 > s5cmd {{.HelpName}} --all-versions "s3://bucket/prefix*"
		
	10. List all versions of all objects in the bucket
		 > s5cmd {{.HelpName}} --all-versions "s3://bucket/*"

	11. List all files only with their fullpaths 
		 > s5cmd {{.HelpName}} --show-fullpath "s3://bucket/*"

`

func NewListCommand() *cli.Command {
	cmd := &cli.Command{
		Name:               "ls",
		HelpName:           "ls",
		Usage:              "list buckets and objects",
		CustomHelpTemplate: listHelpTemplate,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "etag",
				Aliases: []string{"e"},
				Usage:   "show entity tag (ETag) in the output",
			},
			&cli.BoolFlag{
				Name:    "humanize",
				Aliases: []string{"H"},
				Usage:   "human-readable output for object sizes",
			},
			&cli.BoolFlag{
				Name:    "storage-class",
				Aliases: []string{"s"},
				Usage:   "display full name of the object class",
			},
			&cli.StringSliceFlag{
				Name:  "exclude",
				Usage: "exclude objects with given pattern",
			},
			&cli.BoolFlag{
				Name:  "all-versions",
				Usage: "list all versions of object(s)",
			},
			&cli.BoolFlag{
				Name:  "show-fullpath",
				Usage: "shows only the fullpath names of the object(s)",
			},
		},
		Before: func(c *cli.Context) error {
			err := validateLSCommand(c)
			if err != nil {
				printError(commandFromContext(c), c.Command.Name, err)
			}
			return err
		},
		Action: func(c *cli.Context) (err error) {
			defer stat.Collect(c.Command.FullName(), &err)()
			if !c.Args().Present() {
				err := ListBuckets(c.Context, NewStorageOpts(c))
				if err != nil {
					printError(commandFromContext(c), c.Command.Name, err)
				}
				return err
			}

			fullCommand := commandFromContext(c)

			srcurl, err := url.New(c.Args().First(),
				url.WithAllVersions(c.Bool("all-versions")))
			if err != nil {
				printError(fullCommand, c.Command.Name, err)
				return err
			}
			return List{
				src:         srcurl,
				op:          c.Command.Name,
				fullCommand: fullCommand,
				// flags
				showEtag:         c.Bool("etag"),
				humanize:         c.Bool("humanize"),
				showStorageClass: c.Bool("storage-class"),
				exclude:          c.StringSlice("exclude"),
				showFullPath:     c.Bool("show-fullpath"),

				storageOpts: NewStorageOpts(c),
			}.Run(c.Context)
		},
	}

	cmd.BashComplete = getBashCompleteFn(cmd, false, false)
	return cmd
}

// List holds list operation flags and states.
type List struct {
	src         *url.URL
	op          string
	fullCommand string

	// flags
	showEtag         bool
	humanize         bool
	showStorageClass bool
	showFullPath     bool
	exclude          []string

	storageOpts storage.Options
}

// ListBuckets prints all buckets.
func ListBuckets(ctx context.Context, storageOpts storage.Options) error {
	// set as remote storage
	url := &url.URL{Type: 0}
	client, err := storage.NewRemoteClient(ctx, url, storageOpts)
	if err != nil {
		return err
	}

	buckets, err := client.ListBuckets(ctx, "")
	if err != nil {
		return err
	}

	for _, bucket := range buckets {
		log.Info(bucket)
	}

	return nil
}

// Run prints objects at given source.
func (l List) Run(ctx context.Context) error {

	client, err := storage.NewClient(ctx, l.src, l.storageOpts)
	if err != nil {
		printError(l.fullCommand, l.op, err)
		return err
	}

	var merror error

	excludePatterns, err := createExcludesFromWildcard(l.exclude)
	if err != nil {
		printError(l.fullCommand, l.op, err)
		return err
	}

	for object := range client.List(ctx, l.src, false) {
		if errorpkg.IsCancelation(object.Err) {
			continue
		}

		if err := object.Err; err != nil {
			merror = multierror.Append(merror, err)
			printError(l.fullCommand, l.op, err)
			continue
		}

		if isURLExcluded(excludePatterns, object.URL.Path, l.src.Prefix) {
			continue
		}

		msg := ListMessage{
			Object:           object,
			showEtag:         l.showEtag,
			showHumanized:    l.humanize,
			showStorageClass: l.showStorageClass,
			showFullPath:     l.showFullPath,
		}

		log.Info(msg)
	}

	return merror
}

// ListMessage is a structure for logging ls results.
type ListMessage struct {
	Object *storage.Object `json:"object"`

	showEtag         bool
	showHumanized    bool
	showStorageClass bool
	showFullPath     bool
}

// humanize is a helper function to humanize bytes.
func (l ListMessage) humanize() string {
	var size string
	if l.showHumanized {
		size = strutil.HumanizeBytes(l.Object.Size)
	} else {
		size = fmt.Sprintf("%d", l.Object.Size)
	}
	return size
}

const (
	dateFormat = "2006/01/02 15:04:05"
)

// String returns the string representation of ListMessage.
func (l ListMessage) String() string {
	if l.showFullPath {
		return fmt.Sprintf(l.Object.URL.String())
	}
	var etag string
	// date and storage fiels
	var listFormat = "%19s %2s"

	// align etag
	if l.showEtag {
		etag = l.Object.Etag
		listFormat = listFormat + " %-38s"
	} else {
		listFormat = listFormat + " %-1s"
	}

	// format file size
	listFormat = listFormat + " %12s "
	// format key and version ID
	if l.Object.URL.VersionID != "" {
		listFormat = listFormat + " %-50s %s"
	} else {
		listFormat = listFormat + " %s%s"
	}

	var s string
	if l.Object.Type.IsDir() {
		s = fmt.Sprintf(
			listFormat,
			"",
			"",
			"",
			"DIR",
			l.Object.URL.Relative(),
			"",
		)
		return s
	}

	stclass := ""
	if l.showStorageClass {
		stclass = fmt.Sprintf("%v", l.Object.StorageClass)
	}
	s = fmt.Sprintf(
		listFormat,
		l.Object.ModTime.Format(dateFormat),
		stclass,
		etag,
		l.humanize(),
		l.Object.URL.Relative(),
		l.Object.URL.VersionID,
	)

	return s
}

// JSON returns the JSON representation of ListMessage.
func (l ListMessage) JSON() string {
	return strutil.JSON(l.Object)
}

func validateLSCommand(c *cli.Context) error {
	if c.Args().Len() > 1 {
		return fmt.Errorf("expected only 1 argument")
	}

	srcurl, err := url.New(c.Args().First(),
		url.WithAllVersions(c.Bool("all-versions")))
	if err != nil {
		return err
	}

	if err := checkVersinoningURLRemote(srcurl); err != nil {
		return err
	}

	if err := checkVersioningWithGoogleEndpoint(c); err != nil {
		return err
	}

	return nil
}
