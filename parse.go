package s5cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type s3url struct {
	bucket string
	key    string
}

func (s s3url) format() string {
	return s.bucket + "/" + s.key
}

func hasWild(s string) bool {
	return strings.ContainsAny(s, "?*")
}

func parseS3Url(object string) (*s3url, error) {
	if !strings.HasPrefix(object, "s3://") {
		return nil, errors.New("S3 url should start with s3://")
	}
	parts := strings.SplitN(object, "/", 4)
	if parts[2] == "" {
		return nil, errors.New("S3 url should have a bucket")
	}
	if hasWild(parts[2]) {
		return nil, errors.New("Bucket name cannot contain wildcards")
	}
	key := ""
	if len(parts) == 4 {
		key = parts[3]
	}

	return &s3url{
		parts[2],
		key,
	}, nil
}

func parseArgumentByType(s string, t ParamType, fnObj *JobArgument) (*JobArgument, error) {
	fnBase := ""
	if (t == PARAM_S3OBJORDIR || t == PARAM_FILEORDIR) && fnObj != nil {
		fnBase = filepath.Base(fnObj.arg)
	}

	switch t {
	case PARAM_UNCHECKED, PARAM_UNCHECKED_ONE_OR_MORE:
		return &JobArgument{s, nil}, nil

	case PARAM_S3OBJ, PARAM_S3OBJORDIR, PARAM_S3WILDOBJ:
		url, err := parseS3Url(s)
		if err != nil {
			return nil, err
		}

		if (t == PARAM_S3OBJ || t == PARAM_S3OBJORDIR) && hasWild(url.key) {
			return nil, errors.New("S3 key cannot contain wildcards")
		}

		endsInSlash := strings.HasSuffix(url.key, "/")
		if t == PARAM_S3OBJ {
			if endsInSlash {
				return nil, errors.New("S3 key should not end with /")
			}
		}
		if t == PARAM_S3OBJORDIR && endsInSlash && fnBase != "" {
			url.key += fnBase
			s += fnBase
		}
		return &JobArgument{s, url}, nil

	case PARAM_FILEOBJ, PARAM_FILEORDIR, PARAM_DIR:
		// check if we have s3 object
		_, err := parseS3Url(s)
		if err == nil {
			return nil, errors.New("File param resembles s3 object")
		}
		endsInSlash := strings.HasSuffix(s, "/")
		if t == PARAM_FILEOBJ {
			if endsInSlash {
				return nil, errors.New("File param should not end with /")
			}
		}
		if t == PARAM_FILEORDIR && endsInSlash && fnBase != "" {
			s += fnBase
		}
		if t == PARAM_DIR && !endsInSlash {
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

			s += "/"
		}

		return &JobArgument{s, nil}, nil
	}

	return nil, errors.New("Unhandled parseArgumentByType")
}

var (
	// cmd && success-cmd || fail-cmd
	regexCmdAndOr = regexp.MustCompile(`^\s*(.+?)\s*&&\s*(.+?)\s*\|\|\s*(.+?)\s*$`)
	// cmd && success-cmd
	regexCmdAnd = regexp.MustCompile(`^\s*(.+?)\s*&&\s*(.+?)\s*$`)
	// cmd || fail-cmd
	regexCmdOr = regexp.MustCompile(`^\s*(.+?)\s*\|\|\s*(.+?)\s*$`)
)

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

	for {
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
			break
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
			break
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
			break
		}

		j, err = parseSingleJob(jobdesc)
		s = nil
		f = nil
		break
	}
	if err != nil {
		return nil, err
	}

	if j != nil {
		j.successCommand = s
		j.failCommand = f
	}
	return j, nil
}

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

	parts := strings.Split(jobdesc, " ")

	ourJob := &Job{sourceDesc: jobdesc}

	found := -1
	var parseArgErr error = nil
	for i, c := range commands {
		if parts[0] == c.keyword {
			found = i

			if len(c.params) == 1 && c.params[0] == PARAM_UNCHECKED_ONE_OR_MORE && len(parts) > 1 { // special case for exec
				ourJob.command = c.keyword
				ourJob.operation = c.operation
				ourJob.args = []*JobArgument{}

				for i, s := range parts {
					if i == 0 {
						continue
					}
					ourJob.args = append(ourJob.args, &JobArgument{s, nil})
				}

				return ourJob, nil
			}

			if len(parts)-1 != len(c.params) { // check if param counts match
				continue
			}

			ourJob.command = c.keyword
			ourJob.operation = c.operation
			ourJob.args = []*JobArgument{}

			var fnObj *JobArgument

			parseArgErr = nil
			for i, t := range c.params { // check if param types match
				var a *JobArgument
				a, parseArgErr = parseArgumentByType(parts[i+1], t, fnObj)
				if parseArgErr != nil {
					break
				}
				ourJob.args = append(ourJob.args, a)

				if (t == PARAM_S3OBJ || t == PARAM_FILEOBJ) && fnObj == nil {
					fnObj = a
				}
			}
			if parseArgErr != nil {
				continue // not our command, try another
			}

			return ourJob, nil
		}
	}

	if found >= 0 {
		if parseArgErr != nil {
			return nil, fmt.Errorf(`Invalid parameters to "%s": %s`, commands[found].keyword, parseArgErr.Error())
		}
		return nil, fmt.Errorf(`Invalid parameters to "%s"`, parts[0])
	}
	return nil, fmt.Errorf(`Unknown command "%s"`, parts[0])
}
