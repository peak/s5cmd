package command

import (
	"fmt"

	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
	"github.com/urfave/cli/v2"
)

var abortmpHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] object-path upload-id

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Abort multipart upload
		> s5cmd {{.HelpName}} s3://bucket/object 01000191-daf9-7547-5278-71bd81953ffe
`

func NewAbortMultipartCommand() *cli.Command {
	cmd := &cli.Command{
		Name:               "abortmp",
		HelpName:           "abortmp",
		Usage:              "abort multipart uploads",
		CustomHelpTemplate: abortmpHelpTemplate,
		Flags:              []cli.Flag{},
		Before: func(c *cli.Context) error {
			err := validateAbortMultipartCommand(c)
			if err != nil {
				printError(commandFromContext(c), c.Command.Name, err)
			}
			return err
		},
		Action: func(c *cli.Context) (err error) {

			// var merror error

			fullCommand := commandFromContext(c)

			objurl, err := url.New(c.Args().First())
			if err != nil {
				printError(fullCommand, c.Command.Name, err)
				return err
			}
			uploadID := c.Args().Get(1)

			client, err := storage.NewRemoteClient(c.Context, objurl, NewStorageOpts(c))
			if err != nil {
				printError(fullCommand, c.Command.Name, err)
				return err
			}

			err = client.AbortMultipartUpload(c.Context, objurl, uploadID)
			if err != nil && err != storage.ErrNoObjectFound {
				printError(fullCommand, c.Command.Name, err)
				return err
			}

			return nil
		},
	}

	cmd.BashComplete = getBashCompleteFn(cmd, false, false)
	return cmd
}

func validateAbortMultipartCommand(c *cli.Context) error {
	if c.Args().Len() != 2 {
		return fmt.Errorf("expected object path and upload id arguments")
	}

	objectPath := c.Args().Get(0)
	uploadID := c.Args().Get(1)

	_, err := url.New(objectPath)
	if err != nil {
		return err
	}

	if uploadID == "" {
		return fmt.Errorf("expected upload id, got empty string")
	}

	return nil
}
