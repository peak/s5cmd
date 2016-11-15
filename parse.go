package s5cmd

import (
	"errors"
	"fmt"
	"log"
	"net/url"
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

func parseS3Url(object string) (*s3url, error) {
	u, err := url.Parse(object)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "s3" && u.Scheme != "S3" {
		return nil, fmt.Errorf("Invalid URL scheme, must be s3:// but found %s", u.Scheme)
	}
	return &s3url{
		u.Host,
		strings.TrimLeft(u.Path, "/"),
	}, nil
}

func parseArgumentByType(s string, t ParamType) (*JobArgument, error) {
	switch t {
	case PARAM_UNCHECKED, PARAM_UNCHECKED_ONE_OR_MORE:
		return &JobArgument{s, nil}, nil

	case PARAM_S3OBJ, PARAM_S3OBJORDIR:
		url, err := parseS3Url(s)
		if err != nil {
			return nil, err
		}
		if t == PARAM_S3OBJ {
			if strings.HasSuffix(url.key, "/") {
				return nil, errors.New("S3 key should not end with /")
			}
		}
		return &JobArgument{s, url}, nil

	case PARAM_FILEOBJ, PARAM_FILEORDIR:
		// check if we have s3 object
		_, err := parseS3Url(s)
		if err == nil {
			return nil, errors.New("File param resembles s3 object")
		}
		if t == PARAM_FILEOBJ {
			if strings.HasSuffix(s, "/") {
				return nil, errors.New("File param should not end with /")
			}
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
	if jobdesc == "" || strings.HasPrefix(jobdesc, "#") {
		return nil, nil // errors.New("Empty job description")
	}

	if strings.Contains(jobdesc, "&&") {
		return nil, errors.New("Nested commands are not supported")
	}
	if strings.Contains(jobdesc, "||") {
		return nil, errors.New("Nested commands are not supported")
	}

	parts := strings.Split(jobdesc, " ")

	ourJob := &Job{sourceDesc: jobdesc}

	found := false
	for _, c := range commands {
		if parts[0] == c.keyword {
			found = true

			if len(c.params) == 1 && c.params[0] == PARAM_UNCHECKED_ONE_OR_MORE { // special case for exec
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

			ourJob.operation = c.operation
			ourJob.args = []*JobArgument{}

			var err error = nil
			for i, t := range c.params { // check if param types match
				var a *JobArgument
				a, err = parseArgumentByType(parts[i+1], t)
				if err != nil {
					log.Print("Error parsing arg ", t, err)
					break
				}
				ourJob.args = append(ourJob.args, a)

			}
			if err != nil {
				continue // not our command, try another
			}

			return ourJob, nil
		}
	}

	if found {
		return nil, fmt.Errorf(`Invalid parameters for command "%s"`, parts[0])
	}
	return nil, fmt.Errorf(`Unknown command "%s"`, parts[0])
}
