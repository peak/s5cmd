package command

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/urfave/cli/v2"
)

func commandFromContext(c *cli.Context) string {
	cmd := c.Command.FullName()

	for _, f := range c.Command.Flags {
		flagname := f.Names()[0]
		for _, flagvalue := range contextValue(c, flagname) {
			cmd = fmt.Sprintf("%s --%s=%v", cmd, flagname, flagvalue)
		}
	}

	if c.Args().Len() > 0 {
		cmd = fmt.Sprintf("%v %v", cmd, strings.Join(c.Args().Slice(), " "))
	}

	return cmd
}

// contextValue traverses context and its ancestor contexts to find
// the flag value and returns string slice.
func contextValue(c *cli.Context, flagname string) []string {
	for _, c := range c.Lineage() {
		if !c.IsSet(flagname) {
			continue
		}

		val := c.Value(flagname)
		switch val.(type) {
		case cli.StringSlice:
			return c.StringSlice(flagname)
		case cli.Int64Slice:
			values := c.Int64Slice(flagname)
			var result []string
			for _, v := range values {
				result = append(result, strconv.FormatInt(v, 10))
			}
			return result
		case string:
			return []string{c.String(flagname)}
		case bool:
			return []string{strconv.FormatBool(c.Bool(flagname))}
		case int:
			return []string{strconv.Itoa(c.Int(flagname))}
		default:
			return []string{fmt.Sprintf("%v", val)}
		}
	}

	return nil
}
