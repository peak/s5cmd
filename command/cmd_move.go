package command

import (
	"fmt"

	"github.com/peak/s5cmd/storage"
	"github.com/urfave/cli/v2"
)

var MoveCommand = &cli.Command{
	Name:     "mv",
	HelpName: "move",
	Usage:    "move objects",
	Flags:    copyCommandFlags, // move and copy commands share the same flags
	Before: func(c *cli.Context) error {
		if c.Args().Len() < 2 {
			return fmt.Errorf("expected source and destination arguments")
		}

		args := c.Args().Slice()
		last := c.Args().Len() - 1
		src := args[:last]
		if err := checkSources(src...); err != nil {
			return err
		}
		return nil
	},
	Action: func(c *cli.Context) error {
		noClobber := c.Bool("no-clobber")
		ifSizeDiffer := c.Bool("if-size-differ")
		ifSourceNewer := c.Bool("if-source-newer")
		recursive := c.Bool("recursive")
		parents := c.Bool("parents")
		storageClass := storage.LookupClass(c.String("storage-class"))

		args := c.Args().Slice()
		last := c.Args().Len() - 1
		src := args[:last]
		dst := args[last]

		return Copy(
			c.Context,
			src,
			dst,
			c.Command.Name,
			givenCommand(c),
			true, // delete source
			// flags
			noClobber,
			ifSizeDiffer,
			ifSourceNewer,
			recursive,
			parents,
			storageClass,
		)
	},
}
