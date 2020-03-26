package command

import (
	"github.com/peak/s5cmd/storage"
	"github.com/urfave/cli/v2"
)

var moveHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] source destination

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Move an S3 object to working directory
		 > s5cmd {{.HelpName}} s3://bucket/prefix/object.gz .

	2. Move an S3 object and rename
		 > s5cmd {{.HelpName}} s3://bucket/prefix/object.gz myobject.gz

	3. Move all S3 objects to a directory
		 > s5cmd {{.HelpName}} s3://bucket/* target-directory/

	4. Move a file to S3 bucket
		 > s5cmd {{.HelpName}} myfile.gz s3://bucket/

	5. Move a directory to S3 bucket recursively
		 > s5cmd {{.HelpName}} dir/ s3://bucket/
`

var MoveCommand = &cli.Command{
	Name:               "mv",
	HelpName:           "mv",
	Usage:              "move objects",
	Flags:              copyCommandFlags, // move and copy commands share the same flags
	CustomHelpTemplate: moveHelpTemplate,
	Before: func(c *cli.Context) error {
		return CopyCommand.Before(c)
	},
	Action: func(c *cli.Context) error {
		copyCommand := Copy{
			src:          c.Args().Get(0),
			dst:          c.Args().Get(1),
			op:           c.Command.Name,
			fullCommand:  givenCommand(c),
			deleteSource: true, // delete source
			// flags
			noClobber:     c.Bool("no-clobber"),
			ifSizeDiffer:  c.Bool("if-size-differ"),
			ifSourceNewer: c.Bool("if-source-newer"),
			flatten:       c.Bool("flatten"),
			storageClass:  storage.LookupClass(c.String("storage-class")),
		}

		return copyCommand.Run(c.Context)
	},
}
