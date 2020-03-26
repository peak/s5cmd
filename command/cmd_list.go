package command

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/urfave/cli/v2"

	errorpkg "github.com/peak/s5cmd/error"
	"github.com/peak/s5cmd/log"
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

var ListCommand = &cli.Command{
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

		return List(
			c.Context,
			c.Args().First(),
			showEtag,
			humanize,
		)
	},
}

func ListBuckets(ctx context.Context) error {
	// set as remote storage
	url := &url.URL{Type: 0}
	client, err := storage.NewClient(url)
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

func List(
	ctx context.Context,
	src string,
	// flags
	showEtag bool,
	humanize bool,
) error {
	srcurl, err := url.New(src)
	if err != nil {
		return err
	}

	client, err := storage.NewClient(srcurl)
	if err != nil {
		return err
	}

	var merror error

	for object := range client.List(ctx, srcurl) {
		if errorpkg.IsCancelation(object.Err) {
			continue
		}

		if err := object.Err; err != nil {
			merror = multierror.Append(merror, err)
			continue
		}

		msg := ListMessage{
			Object:        object,
			showEtag:      showEtag,
			showHumanized: humanize,
		}

		log.Info(msg)
	}

	return merror
}

// ListMessage is a structure for logging ls results.
type ListMessage struct {
	Object *storage.Object `json:"object"`

	showEtag      bool
	showHumanized bool
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
	listFormat = "%19s %1s %-6s %12s %s"
	dateFormat = "2006/01/02 15:04:05"
)

// String returns the string representation of ListMessage.
func (l ListMessage) String() string {
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

	var etag string
	if l.showEtag {
		etag = l.Object.Etag
	}

	s := fmt.Sprintf(
		listFormat,
		l.Object.ModTime.Format(dateFormat),
		l.Object.StorageClass.ShortCode(),
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
