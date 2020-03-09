package core

import (
	"context"

	"github.com/peak/s5cmd/objurl"
	"github.com/urfave/cli/v2"
)

var MoveCommand = &cli.Command{
	Name:     "mv",
	HelpName: "move",
	Usage:    "TODO",
	Flags: []cli.Flag{
		&cli.BoolFlag{Name: "no-clobber", Aliases: []string{"n"}},
		&cli.BoolFlag{Name: "if-size-differ", Aliases: []string{"s"}},
		&cli.BoolFlag{Name: "if-source-newer", Aliases: []string{"u"}},
		&cli.BoolFlag{Name: "parents"},
		&cli.BoolFlag{Name: "recursive", Aliases: []string{"R"}},
		&cli.StringFlag{Name: "storage-class"},
	},
	Before: func(c *cli.Context) error {
		return validateArguments(c)
	},
	OnUsageError: func(c *cli.Context, err error, isSubcommand bool) error {
		if err != nil {
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
		storageClass := c.String("storage-class")

		return Copy(
			c.Context,
			c.Args().Get(0),
			c.Args().Get(1),
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

func Move(
	ctx context.Context,
	src string,
	dst string,
	// flags
	noClobber bool,
	ifSizeDiffer bool,
	ifSourceNewer bool,
	recursive bool,
	parents bool,
	storageClass string,
) error {
	srcurl, err := objurl.New(src)
	if err != nil {
		return err
	}

	dsturl, err := objurl.New(dst)
	if err != nil {
		return err
	}

	if err := CheckConditions(
		ctx,
		srcurl,
		dsturl,
		noClobber,
		ifSizeDiffer,
		ifSourceNewer,
	); err != nil {
		return err
	}

	return nil
	// FIXME(ig):
}
