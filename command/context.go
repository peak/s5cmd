package command

import (
	"flag"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/peak/s5cmd/storage/url"
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
		case cli.Int64Slice, cli.IntSlice:
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
		case int, int64:
			return []string{strconv.FormatInt(c.Int64(flagname), 10)}
		default:
			return []string{fmt.Sprintf("%v", val)}
		}
	}

	return nil
}

// generateCommand generates command string from given context, app command, default flags and urls.
func generateCommand(c *cli.Context, cmd string, defaultFlags map[string]interface{}, urls ...*url.URL) (string, error) {
	command := AppCommand(cmd)
	flagset := flag.NewFlagSet(command.Name, flag.ContinueOnError)

	var args []string
	for _, url := range urls {
		args = append(args, fmt.Sprintf("%q", url.String()))
	}

	flags := []string{}
	for flagname, flagvalue := range defaultFlags {
		flags = append(flags, fmt.Sprintf("--%s=%v", flagname, flagvalue))
	}

	isDefaultFlag := func(flagname string) bool {
		_, ok := defaultFlags[flagname]
		return ok
	}

	for _, f := range command.Flags {
		flagname := f.Names()[0]
		if isDefaultFlag(flagname) || !c.IsSet(flagname) {
			continue
		}

		for _, flagvalue := range contextValue(c, flagname) {
			flags = append(flags, fmt.Sprintf("--%s=%s", flagname, flagvalue))
		}
	}

	sort.Strings(flags)
	flags = append(flags, args...)
	flags = append([]string{command.Name}, flags...)

	err := flagset.Parse(flags)
	if err != nil {
		return "", err
	}

	cmdCtx := cli.NewContext(c.App, flagset, c)
	return strings.TrimSpace(commandFromContext(cmdCtx)), nil
}
