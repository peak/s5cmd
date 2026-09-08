package command

import (
	"context"
	"github.com/peak/s5cmd/v2/log/stat"

	"github.com/urfave/cli/v3"
)

var moveHelpTemplate = `Name:
	{{.FullName}} - {{.Usage}}

Usage:
	{{.FullName}} [options] source destination

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Move an S3 object to working directory
		 > {{.FullName}} s3://bucket/prefix/object.gz .

	2. Move an S3 object and rename
		 > {{.FullName}} s3://bucket/prefix/object.gz myobject.gz

	3. Move all S3 objects to a directory
		 > {{.FullName}} "s3://bucket/*" target-directory/

	4. Move a file to S3 bucket
		 > {{.FullName}} myfile.gz s3://bucket/

	5. Move a directory to S3 bucket recursively
		 > {{.FullName}} dir/ s3://bucket/

	6. Move all files to S3 bucket but exclude the ones with txt and gz extension
		 > {{.FullName}} --exclude "*.txt" --exclude "*.gz" dir/ s3://bucket

	7. Move all files from S3 bucket to another S3 bucket but exclude the ones starts with log
		 > {{.FullName}} --exclude "log*" "s3://bucket/*" s3://destbucket
`

func NewMoveCommand() *cli.Command {
	cmd := &cli.Command{
		Name:               "mv",
		Usage:              "move/rename objects",
		Flags:              NewCopyCommandFlags(), // move and copy commands share the same flags
		CustomHelpTemplate: moveHelpTemplate,
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			return NewCopyCommand().Before(ctx, c)
		},
		Action: func(ctx context.Context, c *cli.Command) (err error) {
			defer stat.Collect(commandName(c), &err)()

			// delete source
			copy, err := NewCopy(c, true)
			if err != nil {
				return err
			}
			return copy.Run(ctx)
		},
	}

	cmd.ShellComplete = getBashCompleteFn(cmd, false, false)
	return cmd
}
