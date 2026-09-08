package command

import (
	"context"
	"fmt"

	"github.com/urfave/cli/v3"

	"github.com/peak/s5cmd/v2/version"
)

func NewVersionCommand() *cli.Command {
	return &cli.Command{
		Name:  "version",
		Usage: "print version",
		Action: func(ctx context.Context, c *cli.Command) error {
			fmt.Println(version.GetHumanVersion())
			return nil
		},
	}
}
