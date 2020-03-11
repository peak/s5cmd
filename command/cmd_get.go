package command

import (
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/storage"
)

var GetCommand = &cli.Command{
	Name:     "get",
	HelpName: "get",
	Usage:    "TODO",
	Flags:    copyCommandFlags,
	Before: func(c *cli.Context) error {
		validate := func() error {
			arglen := c.Args().Len()
			if arglen == 0 {
				return fmt.Errorf("source is required")
			}
			if arglen > 2 {
				return fmt.Errorf("too many arguments: expecting source and destination path")
			}
			return nil
		}

		if err := validate(); err != nil {
			printError(givenCommand(c), c.Command.Name, err)
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

		dst := "."
		if c.Args().Len() == 2 {
			dst = c.Args().Get(1)
		}

		err := Copy(
			c.Context,
			c.Args().Get(0),
			dst,
			c.Command.Name,
			false, // don't delete source
			// flags
			noClobber,
			ifSizeDiffer,
			ifSourceNewer,
			recursive,
			parents,
			storageClass,
		)
		if err != nil {
			printError(givenCommand(c), c.Command.Name, err)
			return err
		}

		return nil
	},
}
