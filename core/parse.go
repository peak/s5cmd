// Package core is the core package for s5cmd.
package core

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/url"
)

const (
	// GlobCharacters is valid glob characters for local files
	GlobCharacters string = "?*["
)

var (
	// cmd && success-cmd || fail-cmd
	regexCmdAndOr = regexp.MustCompile(`^\s*(.+?)\s*&&\s*(.+?)\s*\|\|\s*(.+?)\s*$`)
	// cmd && success-cmd
	regexCmdAnd = regexp.MustCompile(`^\s*(.+?)\s*&&\s*(.+?)\s*$`)
	// cmd || fail-cmd
	regexCmdOr = regexp.MustCompile(`^\s*(.+?)\s*\|\|\s*(.+?)\s*$`)
)

func isGlob(s string) bool {
	return strings.ContainsAny(s, GlobCharacters)
}

// parseArgumentByType attempts to parse an input string according to the given opt.ParamType and returns a JobArgument (or error)
// fnObj is the last/previous successfully parsed argument, used mainly to append the basenames of the source files to destination directories
func parseArgumentByType(s string, t opt.ParamType, fnObj *JobArgument) (*JobArgument, error) {
	fnBase := ""
	if (t == opt.S3ObjOrDir || t == opt.FileOrDir || t == opt.OptionalFileOrDir) && fnObj != nil {
		fnBase = filepath.Base(fnObj.arg)
	}

	switch t {
	case opt.Unchecked, opt.UncheckedOneOrMore:
		return NewJobArgument(s, nil), nil

	case opt.S3Obj, opt.S3ObjOrDir, opt.S3WildObj, opt.S3Dir, opt.S3SimpleObj:
		uri, err := url.ParseS3Url(s)
		if err != nil {
			return nil, err
		}
		s = "s3://" + uri.Format() // rebuild s with formatted url

		if (t == opt.S3Obj || t == opt.S3ObjOrDir || t == opt.S3SimpleObj) && url.HasWild(uri.Key) {
			return nil, errors.New("S3 key cannot contain wildcards")
		}
		if t == opt.S3WildObj {
			if !url.HasWild(uri.Key) {
				return nil, errors.New("S3 key should contain wildcards")
			}
			if uri.Key == "" {
				return nil, errors.New("S3 key should not be empty")
			}
		}

		if t == opt.S3SimpleObj && uri.Key == "" {
			return nil, errors.New("S3 key should not be empty")
		}

		endsInSlash := strings.HasSuffix(uri.Key, "/")
		if endsInSlash {
			if t == opt.S3Obj || t == opt.S3SimpleObj {
				return nil, errors.New("S3 key should not end with /")
			}
		} else {
			if t == opt.S3Dir && uri.Key != "" {
				return nil, errors.New("S3 dir should end with /")
			}
		}
		if t == opt.S3ObjOrDir && endsInSlash && fnBase != "" {
			uri.Key += fnBase
			s += fnBase
		}
		if t == opt.S3ObjOrDir && uri.Key == "" && fnBase != "" {
			uri.Key += fnBase
			s += "/" + fnBase
		}
		return NewJobArgument(s, uri), nil

	case opt.OptionalFileOrDir, opt.OptionalDir:
		if s == "" {
			s = "."
		}
		fallthrough
	case opt.FileObj, opt.FileOrDir, opt.Dir:
		// check if we have s3 object
		_, err := url.ParseS3Url(s)
		if err == nil {
			return nil, errors.New("File param resembles s3 object")
		}
		if s == "." {
			s = "." + string(filepath.Separator)
		}
		endsInSlash := len(s) > 0 && s[len(s)-1] == filepath.Separator

		if isGlob(s) {
			return nil, errors.New("Param should not contain glob characters")
		}

		if t == opt.FileObj {
			if endsInSlash {
				return nil, errors.New("File param should not end with /")
			}
			st, err := os.Stat(s)
			if err == nil && st.IsDir() {
				return nil, errors.New("File param should not be a directory")
			}
		}
		if (t == opt.FileOrDir || t == opt.OptionalFileOrDir) && endsInSlash && fnBase != "" {
			s += fnBase
		}
		if (t == opt.FileOrDir || t == opt.OptionalFileOrDir) && !endsInSlash {
			st, err := os.Stat(s)
			if err != nil {
				if !os.IsNotExist(err) {
					return nil, errors.New("Could not stat")
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
					return nil, errors.New("Could not stat")
				}
			} else {
				if !st.IsDir() {
					return nil, errors.New("Dir param can not be file")
				}
			}

			s += string(filepath.Separator)
		}

		return NewJobArgument(s, nil), nil

	case opt.Glob:
		_, err := url.ParseS3Url(s)
		if err == nil {
			return nil, errors.New("Glob param resembles s3 object")
		}
		if !isGlob(s) {
			return nil, errors.New("Param does not look like a glob")
		}
		_, err = filepath.Match(s, "")
		if err != nil {
			return nil, err
		}
		return NewJobArgument(s, nil), nil

	}

	return nil, errors.New("Unhandled parseArgumentByType")
}

// ParseJob parses a job description and returns a *Job type, possibly with other *Job types in successCommand/failCommand
func ParseJob(jobdesc string) (*Job, error) {

	jobdesc = strings.Split(jobdesc, " #")[0] // Get rid of comments
	jobdesc = strings.TrimSpace(jobdesc)
	// Get rid of double or more spaces
	jobdesc = strings.Replace(jobdesc, "  ", " ", -1)
	jobdesc = strings.Replace(jobdesc, "  ", " ", -1)
	jobdesc = strings.Replace(jobdesc, "  ", " ", -1)

	var (
		j, s, f *Job
		err     error
	)

	res := regexCmdAndOr.FindStringSubmatch(jobdesc)
	if res != nil {
		j, err = parseSingleJob(res[1])
		if err != nil {
			return nil, err
		}

		s, err = parseSingleJob(res[2])
		if err != nil {
			return nil, err
		}

		f, err = parseSingleJob(res[3])
		if err != nil {
			return nil, err
		}
		goto found
	}

	res = regexCmdAnd.FindStringSubmatch(jobdesc)
	if res != nil {
		j, err = parseSingleJob(res[1])
		if err != nil {
			return nil, err
		}

		s, err = parseSingleJob(res[2])
		if err != nil {
			return nil, err
		}
		goto found
	}

	res = regexCmdOr.FindStringSubmatch(jobdesc)
	if res != nil {
		j, err = parseSingleJob(res[1])
		if err != nil {
			return nil, err
		}

		f, err = parseSingleJob(res[2])
		if err != nil {
			return nil, err
		}
		goto found
	}

	j, err = parseSingleJob(jobdesc)
	s = nil
	f = nil
	if err != nil {
		return nil, err
	}

found:
	if j != nil {
		j.successCommand = s
		j.failCommand = f
	}
	return j, nil
}

// parseSingleJob attempts to parse a single job description to a standalone Job struct.
// It will loop through each accepted command-signature, trying to find the first one that fits.
func parseSingleJob(jobdesc string) (*Job, error) {
	if jobdesc == "" || jobdesc[0] == '#' {
		return nil, nil
	}

	if strings.Contains(jobdesc, "&&") {
		return nil, errors.New("Nested commands are not supported")
	}
	if strings.Contains(jobdesc, "||") {
		return nil, errors.New("Nested commands are not supported")
	}

	// Tokenize arguments
	parts := strings.Split(jobdesc, " ")

	var numSuccess, numFails, numAcceptableFails uint32
	// Create a skeleton Job
	ourJob := &Job{sourceDesc: jobdesc, numSuccess: &numSuccess, numFails: &numFails, numAcceptableFails: &numAcceptableFails}

	found := -1
	var parseArgErr error
	for i, c := range Commands {
		if parts[0] == c.Keyword { // The first token is the name of our command, "cp", "mv" etc.
			found = i // Save the id of the last matching command, we will use this in our error message if needed

			// Enrich our skeleton Job with default values for this specific command
			ourJob.command = c.Keyword
			ourJob.operation = c.Operation
			ourJob.args = []*JobArgument{}
			ourJob.opts = c.Opts

			// Parse options below, until endOptParse
			fileArgsStartPosition := 1 // Position where the real file/s3 arguments start. Before this comes the options/flags.
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
						ourJob.opts = append(ourJob.opts, p)
						foundOpt = true
					}
				}
				if !foundOpt { // End option parsing if it looks like an option but isn't/doesn't match the list
					fileArgsStartPosition = k
					goto endOptParse
				}
			}
		endOptParse:

			// Don't parse args if we have the help option
			if ourJob.opts.Has(opt.Help) {
				return ourJob, nil
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
			var a, fnObj *JobArgument

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

				ourJob.args = append(ourJob.args, a)

				if (t == opt.S3Obj || t == opt.S3SimpleObj || t == opt.FileObj) && fnObj == nil {
					fnObj = a
				}
				maxI = i
				lastType = t
			}
			if parseArgErr == nil && minCount != maxCount && maxCount == -1 { // If no error yet, and we have unlimited/repeating parameters...
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
					ourJob.args = append(ourJob.args, a)
				}
			}
			if parseArgErr != nil {
				verboseLog("Our command doesn't look to be a %s", c.String())
				continue // Not our command, try another
			}

			verboseLog("Our command looks to be a %s", c.String(ourJob.opts...))

			return ourJob, nil
		}
	}

	if found >= 0 {
		if parseArgErr != nil {
			return nil, fmt.Errorf(`Invalid parameters to "%s": %s`, Commands[found].Keyword, parseArgErr.Error())
		}
		return nil, fmt.Errorf(`Invalid parameters to "%s"`, parts[0])
	}
	return nil, fmt.Errorf(`Unknown command "%s"`, parts[0])
}
