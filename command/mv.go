package command

import (
	"github.com/peak/s5cmd/v2/log/stat"

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
		 > s5cmd {{.HelpName}} "s3://bucket/*" target-directory/

	4. Move a file to S3 bucket
		 > s5cmd {{.HelpName}} myfile.gz s3://bucket/

	5. Move a directory to S3 bucket recursively
		 > s5cmd {{.HelpName}} dir/ s3://bucket/

	6. Move all files to S3 bucket but exclude the ones with txt and gz extension
		 > s5cmd {{.HelpName}} --exclude "*.txt" --exclude "*.gz" dir/ s3://bucket

	7. Move all files from S3 bucket to another S3 bucket but exclude the ones starts with log
		 > s5cmd {{.HelpName}} --exclude "log*" "s3://bucket/*" s3://destbucket
`

func NewMoveCommand() *cli.Command {
	cmd := &cli.Command{
		Name:               "mv",
		HelpName:           "mv",
		Usage:              "move/rename objects",
		Flags:              NewCopyCommandFlags(), // move and copy commands share the same flags
		CustomHelpTemplate: moveHelpTemplate,
		Before: func(c *cli.Context) error {
			return NewCopyCommand().Before(c)
		},
		Action: func(c *cli.Context) (err error) {
			defer stat.Collect(c.Command.FullName(), &err)()

			// delete source
			copy, err := NewCopy(c, true)
			if err != nil {
				return err
			}
			return copy.Run(c.Context)
		},
	}

	cmd.BashComplete = getBashCompleteFn(cmd, false, false)
	return cmd
}
