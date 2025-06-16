package command

import (
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
	"github.com/peak/s5cmd/v2/strutil"
	"github.com/urfave/cli/v2"
)

var partsHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] object_path uploadID

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. List multipart upload parts
		> s5cmd {{.HelpName}} s3://bucket/object 0a6d5ad3-3cab-4d88-aa8b-b735de98877f
`

func NewMultipartPartsCommand() *cli.Command {
	cmd := &cli.Command{
		Name:               "parts",
		HelpName:           "parts",
		Usage:              "list multipart upload parts",
		CustomHelpTemplate: partsHelpTemplate,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "humanize",
				Aliases: []string{"H"},
				Usage:   "human-readable output for object sizes",
			},
		},
		// Before: func(c *cli.Context) error {
		// 	err := validateListMultipartCommand(c)
		// 	if err != nil {
		// 		printError(commandFromContext(c), c.Command.Name, err)
		// 	}
		// 	return err
		// },
		Action: func(c *cli.Context) (err error) {

			var merror error

			fullCommand := commandFromContext(c)

			parturl, err := url.New(c.Args().First())
			if err != nil {
				printError(fullCommand, c.Command.Name, err)
				return err
			}
			uploadID := c.Args().Get(1)

			client, err := storage.NewRemoteClient(c.Context, parturl, NewStorageOpts(c))
			if err != nil {
				printError(fullCommand, c.Command.Name, err)
				return err
			}

			for part := range client.ListMultipartUploadParts(c.Context, parturl, uploadID) {
				if err := part.Err; err != nil {
					merror = multierror.Append(merror, err)
					printError(fullCommand, c.Command.Name, err)
					continue
				}
				msg := MPPartsMessage{
					Part:          part,
					showHumanized: c.Bool("humanize"),
				}
				log.Info(msg)
			}

			return nil
		},
	}

	cmd.BashComplete = getBashCompleteFn(cmd, false, false)
	return cmd
}

type MPPartsMessage struct {
	Part *storage.MPPartObject `json:"part"`

	showHumanized bool
}

func (pm MPPartsMessage) humanize() string {
	var size string
	if pm.showHumanized {
		size = strutil.HumanizeBytes(pm.Part.Size)
	} else {
		size = fmt.Sprintf("%d", pm.Part.Size)
	}
	return size
}

// String returns the string representation of ListMessage.
func (pm MPPartsMessage) String() string {
	// date
	var listFormat = "%19s"

	// Part number, etag, size
	listFormat = listFormat + " %d %s %s"

	s := fmt.Sprintf(
		listFormat,
		pm.Part.ModTime.Format(dateFormat),
		pm.Part.PartNumber,
		pm.Part.ETag,
		pm.humanize(),
	)

	return s
}

// JSON returns the JSON representation of ListMessage.
func (pm MPPartsMessage) JSON() string {
	return strutil.JSON(pm.Part)
}
