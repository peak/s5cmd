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

var sizeHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] argument

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Show disk usage of all objects in a bucket
		 > s5cmd {{.HelpName}} s3://bucket/*

	2. Show disk usage of all objects that match a wildcard, grouped by storage class
		 > s5cmd {{.HelpName}} --group s3://bucket/prefix/obj*.gz
`

var sizeCommand = &cli.Command{
	Name:               "du",
	HelpName:           "du",
	Usage:              "show object size usage",
	CustomHelpTemplate: sizeHelpTemplate,
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:    "group",
			Aliases: []string{"g"},
			Usage:   "group sizes by storage class",
		},
		&cli.BoolFlag{
			Name:    "humanize",
			Aliases: []string{"H"},
			Usage:   "human-readable output for object sizes",
		},
	},
	Before: func(c *cli.Context) error {
		if c.Args().Len() != 1 {
			return fmt.Errorf("expected only 1 argument")
		}
		return nil
	},
	Action: func(c *cli.Context) error {
		groupByClass := c.Bool("group")
		humanize := c.Bool("humanize")

		return Size(
			c.Context,
			c.Args().First(),
			groupByClass,
			humanize,
		)
	},
}

// Size calculates disk usage of given source.
func Size(
	ctx context.Context,
	src string,
	groupByClass bool,
	humanize bool,
) error {
	srcurl, err := url.New(src)
	if err != nil {
		return err
	}

	client, err := storage.NewClient(srcurl, AppStorageOptions)
	if err != nil {
		return err
	}

	storageTotal := map[string]sizeAndCount{}
	total := sizeAndCount{}

	var merror error

	for object := range client.List(ctx, srcurl, false) {
		if object.Type.IsDir() || errorpkg.IsCancelation(object.Err) {
			continue
		}

		if err := object.Err; err != nil {
			merror = multierror.Append(merror, err)
			continue
		}
		storageClass := string(object.StorageClass)
		s := storageTotal[storageClass]
		s.addObject(object)
		storageTotal[storageClass] = s

		total.addObject(object)
	}

	if !groupByClass {
		msg := SizeMessage{
			Source:        srcurl.String(),
			Count:         total.count,
			Size:          total.size,
			showHumanized: humanize,
		}
		log.Info(msg)
		return nil
	}

	for k, v := range storageTotal {
		msg := SizeMessage{
			Source:        srcurl.String(),
			StorageClass:  k,
			Count:         v.count,
			Size:          v.size,
			showHumanized: humanize,
		}
		log.Info(msg)
	}

	return merror
}

// SizeMessage is the structure for logging disk usage.
type SizeMessage struct {
	Source       string `json:"source"`
	StorageClass string `json:"storage_class,omitempty"`
	Count        int64  `json:"count"`
	Size         int64  `json:"size"`

	showHumanized bool
}

// humanize is a helper method to humanize bytes.
func (s SizeMessage) humanize() string {
	if s.showHumanized {
		return strutil.HumanizeBytes(s.Size)
	}
	return fmt.Sprintf("%d", s.Size)
}

// String returns the string representation of SizeMessage.
func (s SizeMessage) String() string {
	var storageCls string
	if s.StorageClass != "" {
		storageCls = fmt.Sprintf(" [%s]", s.StorageClass)
	}
	return fmt.Sprintf(
		"%s bytes in %d objects: %s%s",
		s.humanize(),
		s.Count,
		s.Source,
		storageCls,
	)
}

// JSON returns the JSON representation of SizeMessage.
func (s SizeMessage) JSON() string {
	return strutil.JSON(s)
}

type sizeAndCount struct {
	size  int64
	count int64
}

func (s *sizeAndCount) addObject(obj *storage.Object) {
	s.size += obj.Size
	s.count++
}
