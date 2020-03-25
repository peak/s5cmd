package command

import (
	"github.com/peak/s5cmd/storage"
	"github.com/urfave/cli/v2"
)

var MoveCommand = &cli.Command{
	Name:     "mv",
	HelpName: "mv",
	Usage:    "move objects",
	Flags:    copyCommandFlags, // move and copy commands share the same flags
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
			parents:       c.Bool("parents"),
			storageClass:  storage.LookupClass(c.String("storage-class")),
		}

		return copyCommand.Run(c.Context)
	},
}
