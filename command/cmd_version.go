package command

import (
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/version"
)

var VersionCommand = &cli.Command{
	Name:     "version",
	HelpName: "version",
	Usage:    "TODO",
	Action: func(c *cli.Context) error {
		fmt.Println(version.GetHumanVersion())
		return nil
	},
}
