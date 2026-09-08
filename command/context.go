package command

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/kballard/go-shellquote"
	"github.com/urfave/cli/v3"

	"github.com/peak/s5cmd/v2/storage/url"
)

// commandName is the command's name without the binary prefix that v3's
// FullName includes: "cp", or "select csv" for a nested command. v2's
// Command.FullName returned exactly this, and it is what the stat keys and
// error messages have always used.
func commandName(c *cli.Command) string {
	path := c.Path()
	if len(path) < 2 {
		return c.Name
	}
	path = path[1:]

	// Run-mode executes each line of its input as a child of the run
	// command, so v3 reports "run cp ..." for a command the user wrote as
	// "cp ...". The run command has no sub-commands of its own, so a "run"
	// ancestor is always this synthetic one.
	if len(path) > 1 && path[0] == "run" {
		path = path[1:]
	}

	return strings.Join(path, " ")
}

func commandFromContext(c *cli.Command) string {
	// v3's FullName is the whole path including the binary, and the root
	// command's path is just the binary. s5cmd names the sub-command alone
	// ("cp s3://a s3://b"), and names nothing at all for a failure in the
	// root command.
	if len(c.Path()) < 2 {
		return ""
	}
	cmd := commandName(c)

	for _, f := range c.Flags {
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

// contextValue traverses the command and its ancestors to find
// the flag value and returns string slice.
func contextValue(c *cli.Command, flagname string) []string {
	for _, c := range c.Lineage() {
		if !c.IsSet(flagname) {
			continue
		}

		// v3 hands back native values where v2 used its own
		// cli.StringSlice / cli.IntSlice wrappers, and its accessors are
		// type-asserted -- Int64 on an int-typed flag yields 0 -- so each
		// width needs its own case.
		val := c.Value(flagname)
		switch val.(type) {
		case []string:
			return c.StringSlice(flagname)
		case []int:
			var result []string
			for _, v := range c.IntSlice(flagname) {
				result = append(result, strconv.Itoa(v))
			}
			return result
		case []int64:
			var result []string
			for _, v := range c.Int64Slice(flagname) {
				result = append(result, strconv.FormatInt(v, 10))
			}
			return result
		case string:
			return []string{c.String(flagname)}
		case bool:
			return []string{strconv.FormatBool(c.Bool(flagname))}
		case int:
			return []string{strconv.Itoa(c.Int(flagname))}
		case int64:
			return []string{strconv.FormatInt(c.Int64(flagname), 10)}
		default:
			return []string{fmt.Sprintf("%v", val)}
		}
	}

	return nil
}

// generateCommand generates command string from given command, app command, default flags and urls.
func generateCommand(c *cli.Command, cmd string, defaultFlags map[string]any, urls ...*url.URL) string {
	command := AppCommand(cmd)

	var args []string
	for _, url := range urls {
		// Quote with shellquote, the exact inverse of the shellquote.Split
		// that run-mode uses to read these commands back. Go's %q would
		// turn a literal tab or newline in a key into the two characters
		// \t / \n, which are not escape sequences to a shell parser, so
		// the key would come back corrupted and the object would 404.
		args = append(args, shellquote.Join(url.String()))
	}

	flags := []string{}
	for flagname, flagvalue := range defaultFlags {
		flags = append(flags, fmt.Sprintf("--%s='%v'", flagname, flagvalue))
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
			flags = append(flags, fmt.Sprintf("--%s='%s'", flagname, flagvalue))
		}
	}

	sort.Strings(flags)
	flags = append(flags, args...)
	flags = append([]string{command.Name}, flags...)

	// v2 round-tripped these parts through a throwaway flag.FlagSet and
	// cli.Context to render the string; with no Context type in v3, and the
	// round-trip having only ever rejoined the parts, join them directly.
	return strings.TrimSpace(strings.Join(flags, " "))
}
