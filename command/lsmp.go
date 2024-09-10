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

var lsmpHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] prefix

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. List multipart uploads for bucket
		> s5cmd {{.HelpName}} s3://bucket
	2. List multipart uploads for specific object
		> s5cmd {{.HelpName}} s3://bucket/object
	3. List multipart uploads with full path to the object
		> s5cmd {{.HelpName}} --show-fullpath s3://bucket/object
`

func NewListMultipartCommand() *cli.Command {
	cmd := &cli.Command{
		Name:               "lsmp",
		HelpName:           "lsmp",
		Usage:              "list multipart uploads",
		CustomHelpTemplate: lsmpHelpTemplate,
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "show-fullpath",
				Usage: "show the fullpath names of the object(s)",
			},
		},
		Before: func(c *cli.Context) error {
			err := validateListMultipartCommand(c)
			if err != nil {
				printError(commandFromContext(c), c.Command.Name, err)
			}
			return err
		},
		Action: func(c *cli.Context) (err error) {

			var merror error

			fullCommand := commandFromContext(c)

			srcurl, err := url.New(c.Args().First())
			if err != nil {
				printError(fullCommand, c.Command.Name, err)
				return err
			}

			client, err := storage.NewRemoteClient(c.Context, srcurl, NewStorageOpts(c))
			if err != nil {
				printError(fullCommand, c.Command.Name, err)
				return err
			}

			for object := range client.ListMultipartUploads(c.Context, srcurl) {
				if err := object.Err; err != nil {
					merror = multierror.Append(merror, err)
					printError(fullCommand, c.Command.Name, err)
					continue
				}
				msg := ListMPUploadMessage{
					Object:       object,
					showFullPath: c.Bool("show-fullpath"),
				}
				log.Info(msg)
			}

			return nil
		},
	}

	cmd.BashComplete = getBashCompleteFn(cmd, false, false)
	return cmd
}

type ListMPUploadMessage struct {
	Object *storage.UploadObject `json:"object"`

	showFullPath bool
}

// String returns the string representation of ListMessage.
func (l ListMPUploadMessage) String() string {
	// date and storage fields
	var listFormat = "%19s"

	listFormat = listFormat + " %s %s"

	var s string

	var path string
	if l.showFullPath {
		path = l.Object.URL.String()
	} else {
		path = l.Object.URL.Relative()
	}

	s = fmt.Sprintf(
		listFormat,
		l.Object.Initiated.Format(dateFormat),
		path,
		l.Object.UploadId,
	)

	return s
}

// JSON returns the JSON representation of ListMessage.
func (l ListMPUploadMessage) JSON() string {
	return strutil.JSON(l.Object)
}

func validateListMultipartCommand(c *cli.Context) error {
	if c.Args().Len() != 1 {
		return fmt.Errorf("expected 1 argument")
	}

	_, err := url.New(c.Args().First())
	if err != nil {
		return err
	}
	return nil
}
