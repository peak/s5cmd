// Package core is the core package for s5cmd.
package core

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/opt"
)

const (
	// GlobCharacters is valid glob characters for local files
	GlobCharacters string = "?*["
)

// parseArgumentByType parses an input string according to the given
// opt.ParamType and returns a ObjectURL (or error). fnObj is the
// last/previous successfully parsed argument, used mainly to append the
// basenames of the source files to destination directories.
func parseArgumentByType(s string, t opt.ParamType, fnObj *objurl.ObjectURL) (*objurl.ObjectURL, error) {
	fnBase := ""
	if (t == opt.S3ObjOrDir || t == opt.FileOrDir || t == opt.OptionalFileOrDir) && fnObj != nil {
		fnBase = fnObj.Base()
	}

	switch t {
	case opt.Unchecked, opt.UncheckedOneOrMore:
		return objurl.New(s)

	case opt.S3Obj, opt.S3ObjOrDir, opt.S3WildObj, opt.S3Dir, opt.S3SimpleObj:
		url, err := objurl.New(s)
		if err != nil {
			return nil, err
		}

		if !url.IsRemote() {
			return nil, fmt.Errorf("given argument %q is not a remote path", s)
		}

		s = url.Absolute()

		if (t == opt.S3Obj || t == opt.S3ObjOrDir || t == opt.S3SimpleObj) && objurl.HasGlobCharacter(url.Path) {
			return nil, errors.New("s3 key cannot contain wildcards")
		}

		if t == opt.S3WildObj {
			if !objurl.HasGlobCharacter(url.Path) {
				return nil, errors.New("s3 key should contain wildcards")
			}
			if url.Path == "" {
				return nil, errors.New("s3 key cannot not be empty")
			}
		}

		if t == opt.S3SimpleObj && url.Path == "" {
			return nil, errors.New("s3 key should not be empty")
		}

		endsInSlash := strings.HasSuffix(url.Path, "/")
		if endsInSlash {
			if t == opt.S3Obj || t == opt.S3SimpleObj {
				return nil, errors.New("s3 key should not end with /")
			}
		} else {
			if t == opt.S3Dir && url.Path != "" {
				return nil, errors.New("s3 dir should end with /")
			}
		}
		if t == opt.S3ObjOrDir && endsInSlash && fnBase != "" {
			url.Path += fnBase
			s += fnBase
		}
		if t == opt.S3ObjOrDir && url.Path == "" && fnBase != "" {
			url.Path += fnBase
			s += "/" + fnBase
		}

		return objurl.New(s)

	case opt.OptionalFileOrDir, opt.OptionalDir:
		if s == "" {
			s = "."
		}
		fallthrough
	case opt.FileObj, opt.FileOrDir, opt.Dir:
		// check if we have s3 object
		url, _ := objurl.New(s)
		if url.IsRemote() {
			return nil, errors.New("file param resembles s3 object")
		}

		if s == "." {
			s = "." + string(filepath.Separator)
		}
		endsInSlash := len(s) > 0 && s[len(s)-1] == filepath.Separator

		if objurl.HasGlobCharacter(s) {
			return nil, errors.New("param should not contain glob characters")
		}

		if t == opt.FileObj {
			if endsInSlash {
				return nil, errors.New("file param should not end with /")
			}
			st, err := os.Stat(s)
			if err == nil && st.IsDir() {
				return nil, errors.New("file param should not be a directory")
			}
		}
		if (t == opt.FileOrDir || t == opt.OptionalFileOrDir) && endsInSlash && fnBase != "" {
			s += fnBase
		}
		if (t == opt.FileOrDir || t == opt.OptionalFileOrDir) && !endsInSlash {
			st, err := os.Stat(s)
			if err != nil {
				if !os.IsNotExist(err) {
					return nil, errors.New("could not stat")
				}
			} else {
				if st.IsDir() {
					s += string(filepath.Separator)
				}
			}
		}

		if (t == opt.Dir || t == opt.OptionalDir) && !endsInSlash {
			st, err := os.Stat(s)
			if err != nil {
				if !os.IsNotExist(err) {
					return nil, errors.New("could not stat")
				}
			} else {
				if !st.IsDir() {
					return nil, errors.New("dir param can not be file")
				}
			}

			s += string(filepath.Separator)
		}

		return objurl.New(s)

	case opt.Glob:
		url, _ := objurl.New(s)
		if url.IsRemote() {
			return nil, errors.New("glob param resembles a remote object")
		}

		if !objurl.HasGlobCharacter(s) {
			return nil, errors.New("param does not look like a glob")
		}

		_, err := filepath.Match(s, "")
		if err != nil {
			return nil, err
		}

		return url, nil

	}

	return nil, errors.New("unhandled parseArgumentByType")
}

// ParseCommand parses a command description and returns a Command type.
func ParseCommand(cmd string) (*Command, error) {
	cmd = strings.Split(cmd, " #")[0] // Get rid of comments
	cmd = strings.TrimSpace(cmd)
	// Get rid of double or more spaces
	cmd = strings.Replace(cmd, "  ", " ", -1)
	cmd = strings.Replace(cmd, "  ", " ", -1)
	cmd = strings.Replace(cmd, "  ", " ", -1)

	return parseSingleCommand(cmd)
}

// parseSingleCommand attempts to parse a single command description to a standalone Command struct.
// It will loop through each accepted command-signature, trying to find the first one that fits.
func parseSingleCommand(cmd string) (*Command, error) {
	if cmd == "" || cmd[0] == '#' {
		return nil, nil
	}

	// Tokenize arguments
	parts := strings.Split(cmd, " ")
	command := &Command{original: cmd}

	found := -1
	var parseArgErr error
	for i, c := range Commands {
		if parts[0] == c.Keyword { // The first token is the name of our command, "cp", "mv" etc.
			found = i // Save the id of the last matching command, we will use this in our error message if needed

			command.keyword = c.Keyword
			command.operation = c.Operation
			command.opts = c.Opts
			command.args = make([]*objurl.ObjectURL, 0)

			// Parse options below, until endOptParse

			// Position where the real file/s3 arguments start. Before this comes the
			// options/flags.
			fileArgsStartPosition := 1
			acceptedOpts := c.Operation.GetAcceptedOpts()
			for k := 1; k < len(parts); k++ {
				if parts[k][0] != '-' { // If it doesn't look like an option, end option parsing
					fileArgsStartPosition = k
					goto endOptParse
				}
				foundOpt := false
				for _, p := range *acceptedOpts {
					s := p.GetParam()
					if parts[k] == s {
						command.opts = append(command.opts, p)
						foundOpt = true
					}
				}
				// End option parsing if it looks like an option but
				// isn't/doesn't match the list
				if !foundOpt {
					fileArgsStartPosition = k
					goto endOptParse
				}
			}
		endOptParse:

			// Don't parse args if we have the help option
			if command.opts.Has(opt.Help) {
				return command, nil
			}

			// Check number of arguments
			suppliedParamCount := len(parts) - fileArgsStartPosition // Number of arguments/params (sans options and the command name itself)
			minCount := len(c.Params)                                // Minimum number of parameters needed
			maxCount := minCount                                     // Maximum
			if minCount > 0 && c.Params[minCount-1] == opt.UncheckedOneOrMore {
				maxCount = -1 // Accept unlimited parameters if the last param is opt.UncheckedOneOrMore
			}
			if minCount > 0 && (c.Params[minCount-1] == opt.OptionalDir || c.Params[minCount-1] == opt.OptionalFileOrDir) {
				minCount-- // Optional params are optional
			}

			if suppliedParamCount < minCount || (maxCount > -1 && suppliedParamCount > maxCount) { // Check if param counts are acceptable
				// If the number of parameters does not match, try another command
				continue
			}

			// Parse arguments into JobArguments
			var a, fnObj *objurl.ObjectURL

			parseArgErr = nil
			lastType := opt.UncheckedOneOrMore
			maxI := fileArgsStartPosition
			for i, t := range c.Params { // check if param types match
				partVal := ""
				if fileArgsStartPosition+i < len(parts) {
					partVal = parts[fileArgsStartPosition+i]
				}
				a, parseArgErr = parseArgumentByType(partVal, t, fnObj)
				if parseArgErr != nil {
					verboseLog("Error parsing %s as %s: %s", partVal, t.String(), parseArgErr.Error())
					break
				}
				verboseLog("Parsed %s as %s", partVal, t.String())

				command.args = append(command.args, a)

				if (t == opt.S3Obj || t == opt.S3SimpleObj || t == opt.FileObj) && fnObj == nil {
					fnObj = a
				}
				maxI = i
				lastType = t
			}

			// If no error yet, and we have unlimited/repeating parameters...
			if parseArgErr == nil && minCount != maxCount && maxCount == -1 {
				for i, p := range parts {
					if i <= maxI+1 {
						continue
					}
					a, parseArgErr = parseArgumentByType(p, lastType, fnObj)
					if parseArgErr != nil {
						verboseLog("Error parsing %s as %s: %s", p, lastType.String(), parseArgErr.Error())
						break
					}
					verboseLog("Parsed %s as %s", p, lastType.String())

					command.args = append(command.args, a)
				}
			}
			if parseArgErr != nil {
				verboseLog("Our command doesn't look to be a %s", c.String())
				continue // Not our command, try another
			}

			verboseLog("Our command looks to be a %s", c.String(command.opts...))

			return command, nil
		}
	}

	if found >= 0 {
		if parseArgErr != nil {
			return nil, fmt.Errorf("invalid parameters to %q: %s", Commands[found].Keyword, parseArgErr.Error())
		}
		return nil, fmt.Errorf("invalid parameters to %q", parts[0])
	}
	return nil, fmt.Errorf("unknown command %q", parts[0])
}
