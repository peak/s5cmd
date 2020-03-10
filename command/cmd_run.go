package command

/*

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
)

var RunCommand = &cli.Command{
	Name:     "run",
	HelpName: "run",
	Usage:    "TODO",
	Action: func(c *cli.Context) error {
		reader := os.Stdin
		if c.Args().Len() == 1 {
			f, err := os.Open(c.Args().First())
			if err != nil {
				return err
			}
			defer f.Close()

			reader = f
		}

		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			line := scanner.Text()
			fields := strings.Fields(line)
			if len(fields) == 0 {
				fmt.Println("ERR: invalid command")
				continue
			}

			if fields[0] == "run" {
				fmt.Println("ERR: no sir!")
				continue
			}

			fields = append([]string{"s5cmd"}, fields...)

			err := app.Run(fields)
			if err != nil {
				fmt.Println("ERR:", err)
			}
		}
	},
}
*/
