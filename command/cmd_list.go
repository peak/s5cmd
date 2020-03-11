package command

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/strutil"
)

var ListCommand = &cli.Command{
	Name:     "ls",
	HelpName: "list",
	Usage:    "TODO",
	Flags: []cli.Flag{
		&cli.BoolFlag{Name: "etag", Aliases: []string{"e"}},
		&cli.BoolFlag{Name: "humanize", Aliases: []string{"H"}},
	},
	Before: func(c *cli.Context) error {
		if c.Args().Len() > 1 {
			return fmt.Errorf("expected only 1 argument")
		}
		return nil
	},
	Action: func(c *cli.Context) error {
		if !c.Args().Present() {
			err := ListBuckets(c.Context)
			if err != nil {
				printError(givenCommand(c), c.Command.Name, err)
				return err
			}

			return nil
		}

		showEtag := c.Bool("etag")
		humanize := c.Bool("humanize")

		err := List(
			c.Context,
			c.Args().First(),
			givenCommand(c),
			showEtag,
			humanize,
		)
		if err != nil {
			printError(givenCommand(c), c.Command.Name, err)
			return err
		}

		return nil
	},
}

func ListBuckets(ctx context.Context) error {
	// set as remote storage
	url := &objurl.ObjectURL{Type: 0}
	client, err := storage.NewClient(url)
	if err != nil {
		return err
	}

	buckets, err := client.ListBuckets(ctx, "")
	if err != nil {
		return err
	}

	for _, b := range buckets {
		log.Info(b)
	}

	return nil
}

func List(
	ctx context.Context,
	fullCommand string,
	src string,
	showEtag bool,
	humanize bool,
) error {
	srcurl, err := objurl.New(src)
	if err != nil {
		return err
	}

	client, err := storage.NewClient(srcurl)
	if err != nil {
		return err
	}

	for object := range client.List(ctx, srcurl, true, storage.ListAllItems) {
		if isCancelationError(object.Err) {
			continue
		}

		if err := object.Err; err != nil {
			printError(fullCommand, "list", err)
			continue
		}

		msg := ListMessage{
			Object:        object,
			showEtag:      showEtag,
			showHumanized: humanize,
		}

		log.Info(msg)
	}

	return nil
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
		size = humanizeBytes(l.Object.Size)
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
