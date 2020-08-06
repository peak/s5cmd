package command

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/urfave/cli/v2"

	errorpkg "github.com/peak/s5cmd/error"
	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/log/stat"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
	"github.com/peak/s5cmd/strutil"
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
		 > s5cmd {{.HelpName}} s3://bucket/*

	4. List all objects that matches a wildcard
		 > s5cmd {{.HelpName}} s3://bucket/prefix/*/*.gz
`

var listCommand = &cli.Command{
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
	},
	Before: func(c *cli.Context) error {
		if c.Args().Len() > 1 {
			return fmt.Errorf("expected only 1 argument")
		}
		return nil
	},
	Action: func(c *cli.Context) error {
		if !c.Args().Present() {
			return ListBuckets(c.Context)
		}

		showEtag := c.Bool("etag")
		humanize := c.Bool("humanize")
		showStorageClass := c.Bool("storage-class")

		return List(
			c.Context,
			c.Args().First(),
			showEtag,
			humanize,
			showStorageClass,
		)
	},
}

// ListBuckets prints all buckets.
func ListBuckets(ctx context.Context) error {
	// set as remote storage
	url := &url.URL{Type: 0}
	client := storage.NewClient(url)

	buckets, err := client.ListBuckets(ctx, "")
	if err != nil {
		return err
	}

	for _, bucket := range buckets {
		log.Info(bucket)
	}

	return nil
}

// List prints objects at given source.
func List(
	ctx context.Context,
	src string,
	// flags
	showEtag bool,
	humanize bool,
	showStorageClass bool,
) (err error) {
	defer stat.Collect("List", time.Now(), &err)()
	srcurl, err := url.New(src)
	if err != nil {
		return err
	}

	client := storage.NewClient(srcurl)

	var merror error

	for object := range client.List(ctx, srcurl, false) {
		if errorpkg.IsCancelation(object.Err) {
			continue
		}

		if err := object.Err; err != nil {
			merror = multierror.Append(merror, err)
			continue
		}

		msg := ListMessage{
			Object:           object,
			showEtag:         showEtag,
			showHumanized:    humanize,
			showStorageClass: showStorageClass,
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
	var listFormat = "%19s %2s %-1s %12s %s"
	var etag string
	if l.showEtag {
		etag = l.Object.Etag
		listFormat = "%19s %2s %-38s %12s %s"
	}

	if l.Object.Type.IsDir() {
		s := fmt.Sprintf(
			listFormat,
			"",
			"",
			"",
			"DIR",
			l.Object.URL.Relative(),
		)
		return s
	}

	stclass := ""
	if l.showStorageClass {
		stclass = fmt.Sprintf("%v", l.Object.StorageClass)
	}

	s := fmt.Sprintf(
		listFormat,
		l.Object.ModTime.Format(dateFormat),
		stclass,
		etag,
		l.humanize(),
		l.Object.URL.Relative(),
	)
	return s
}

// JSON returns the JSON representation of ListMessage.
func (l ListMessage) JSON() string {
	return strutil.JSON(l.Object)
}
