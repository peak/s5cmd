package command

import (
	"github.com/peak/s5cmd/parallel"
	"github.com/peak/s5cmd/storage"
	"github.com/urfave/cli/v2"
)

var MoveCommand = &cli.Command{
	Name:     "mv",
	HelpName: "move",
	Usage:    "TODO",
	Flags:    copyCommandFlags, // move and copy commands share the same flags
	Before: func(c *cli.Context) error {
		return validateArguments(c)
	},
	OnUsageError: func(c *cli.Context, err error, isSubcommand bool) error {
		if err != nil {
			// FIXME(ig): change "copy" to "move"
			printError(givenCommand(c), "copy", err)
		}
		return err
	},
	Action: func(c *cli.Context) error {
		noClobber := c.Bool("no-clobber")
		ifSizeDiffer := c.Bool("if-size-differ")
		ifSourceNewer := c.Bool("if-source-newer")
		recursive := c.Bool("recursive")
		parents := c.Bool("parents")
		storageClass := storage.LookupClass(c.String("storage-class"))

		fn := func() error {
			return Copy(
				c.Context,
				c.Args().Get(0),
				c.Args().Get(1),
				c.Command.Name,
				true, // delete source
				// flags
				noClobber,
				ifSizeDiffer,
				ifSourceNewer,
				recursive,
				parents,
				storageClass,
			)
		}

		parallel.Run(fn)
		return nil
	},
}
