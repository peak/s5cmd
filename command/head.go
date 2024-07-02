package command

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/v2/log/stat"
	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
)

var headHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] source

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Print a remote object's metadata
		 > s5cmd {{.HelpName}} s3://bucket/prefix/object

	2. Print specific version of a remote object's metadata
		 > s5cmd {{.HelpName}} --version-id VERSION_ID s3://bucket/prefix/object
`

func NewHeadCommand() *cli.Command {
	cmd := &cli.Command{
		Name:     "head",
		HelpName: "head",
		Usage:    "print remote object metadata",

		CustomHelpTemplate: headHelpTemplate,
		Action: func(c *cli.Context) (err error) {

			//print the command name
			//fmt.Println(c.Command.FullName())

			defer stat.Collect(c.Command.FullName(), &err)()

			//fmt.Println("head command")

			op := c.Command.Name
			//fmt.Println("op: ", op)
			fullCommand := commandFromContext(c)

			//print the command name
			//fmt.Println("fullCommand: ", fullCommand)

			src, err := url.New(c.Args().Get(0), url.WithVersion(c.String("version-id")),
				url.WithRaw(c.Bool("raw")))
			if err != nil {
				printError(fullCommand, op, err)
				return err
			}

			return Head{
				src:         src,
				op:          op,
				fullCommand: fullCommand,
				storageOpts: NewStorageOpts(c),
			}.Run(c.Context)

		},
	}
	cmd.BashComplete = getBashCompleteFn(cmd, true, false)
	return cmd
}

type Head struct {
	src         *url.URL
	op          string
	fullCommand string
	storageOpts storage.Options
}

func (h Head) Run(ctx context.Context) error {
	fmt.Println("Context: ", ctx)
	client, err := storage.NewRemoteClient(ctx, h.src, h.storageOpts)
	if err != nil {
		printError(h.fullCommand, h.op, err)
		return err
	}
	head_response, err := client.Stat(ctx, h.src)
	if err != nil {
		printError(h.fullCommand, h.op, err)
		return err
	}
	fmt.Println("Head response: ", head_response)

	return nil
}
