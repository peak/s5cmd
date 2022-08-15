package command

import (
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/version"
)

func NewVersionCommand() *cli.Command {
	return &cli.Command{
		Name:     "version",
		HelpName: "version",
		Usage:    "print version",
		Action: func(c *cli.Context) error {
			fmt.Println(version.GetHumanVersion())
			return nil
		},
	}
}
