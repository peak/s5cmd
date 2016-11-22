package s5cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	S3_WILD_CHARACTERS string = "?*"
)

type s3url struct {
	bucket string
	key    string
}

func (s s3url) format() string {
	if s.key == "" {
		return s.bucket
	}
	return s.bucket + "/" + s.key
}

func (s s3url) Clone() s3url {
	return s3url{s.bucket, s.key}
}

func hasWild(s string) bool {
	return strings.ContainsAny(s, S3_WILD_CHARACTERS)
}
func hasGlob(s string) bool {
	return strings.ContainsAny(s, "*[]?")
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
		key = strings.TrimLeft(parts[3], "/")
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

	case PARAM_S3OBJ, PARAM_S3OBJORDIR, PARAM_S3WILDOBJ, PARAM_S3DIR:
		url, err := parseS3Url(s)
		if err != nil {
			return nil, err
		}
		s = "s3://" + url.format() // rebuild s with formatted url

		if (t == PARAM_S3OBJ || t == PARAM_S3OBJORDIR) && hasWild(url.key) {
			return nil, errors.New("S3 key cannot contain wildcards")
		}
		if t == PARAM_S3WILDOBJ {
			if !hasWild(url.key) {
				return nil, errors.New("S3 key should contain wildcards")
			}
			if url.key == "" {
				return nil, errors.New("S3 key should not be empty")
			}
		}

		endsInSlash := strings.HasSuffix(url.key, "/")
		if endsInSlash {
			if t == PARAM_S3OBJ {
				return nil, errors.New("S3 key should not end with /")
			}
		} else {
			if t == PARAM_S3DIR && url.key != "" {
				return nil, errors.New("S3 dir should end with /")
			}
		}
		if t == PARAM_S3OBJORDIR && endsInSlash && fnBase != "" {
			url.key += fnBase
			s += fnBase
		}
		if t == PARAM_S3OBJORDIR && url.key == "" && fnBase != "" {
			url.key += fnBase
			s += "/" + fnBase
		}
		return &JobArgument{s, url}, nil

	case PARAM_FILEOBJ, PARAM_FILEORDIR, PARAM_DIR:
		// check if we have s3 object
		_, err := parseS3Url(s)
		if err == nil {
			return nil, errors.New("File param resembles s3 object")
		}
		endsInSlash := strings.HasSuffix(s, "/")

		if hasGlob(s) {
			return nil, errors.New("Param should not contain glob characters")
		}

		if t == PARAM_FILEOBJ {
			if endsInSlash {
				return nil, errors.New("File param should not end with /")
			}
			st, err := os.Stat(s)
			if err == nil && st.IsDir() {
				return nil, errors.New("File param should not be a directory")
			}
		}
		if t == PARAM_FILEORDIR && endsInSlash && fnBase != "" {
			s += fnBase
		}
		if t == PARAM_FILEORDIR && !endsInSlash {
			st, err := os.Stat(s)
			if err != nil {
				if !os.IsNotExist(err) {
					return nil, errors.New("Could not stat")
				}
			} else {
				if st.IsDir() {
					s += "/"
				}
			}
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

	case PARAM_GLOB:
		if !hasGlob(s) {
			return nil, errors.New("Param does not look like a glob")
		}
		_, err := filepath.Match(s, "")
		if err != nil {
			return nil, err
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

	var numSuccess, numFails uint32
	ourJob := &Job{sourceDesc: jobdesc, numSuccess: &numSuccess, numFails: &numFails}

	found := -1
	var parseArgErr error = nil
	for i, c := range commands {
		if parts[0] == c.keyword {
			found = i

			suppliedParamCount := len(parts) - 1
			minCount := len(c.params)
			maxCount := minCount
			if minCount > 0 && c.params[minCount-1] == PARAM_UNCHECKED_ONE_OR_MORE {
				maxCount = -1
			}
			if suppliedParamCount < minCount || (maxCount > -1 && suppliedParamCount > maxCount) { // check if param counts are acceptable
				continue
			}

			ourJob.command = c.keyword
			ourJob.operation = c.operation
			ourJob.args = []*JobArgument{}

			var a, fnObj *JobArgument

			parseArgErr = nil
			lastType := PARAM_UNCHECKED_ONE_OR_MORE
			maxI := 0
			for i, t := range c.params { // check if param types match
				a, parseArgErr = parseArgumentByType(parts[i+1], t, fnObj)
				if parseArgErr != nil {
					break
				}
				ourJob.args = append(ourJob.args, a)

				if (t == PARAM_S3OBJ || t == PARAM_FILEOBJ) && fnObj == nil {
					fnObj = a
				}
				maxI = i
				lastType = t
			}
			if parseArgErr == nil && minCount != maxCount {
				for i, p := range parts {
					if i <= maxI+1 {
						continue
					}
					a, parseArgErr = parseArgumentByType(p, lastType, fnObj)
					if parseArgErr != nil {
						break
					}
					ourJob.args = append(ourJob.args, a)
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
