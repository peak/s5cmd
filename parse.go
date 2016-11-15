package s5cmd

import (
	"errors"
	"fmt"
	"log"
	"net/url"
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
		u.Path,
	}, nil
}

func parseArgumentByType(s string, t ParamType) (*JobArgument, error) {
	switch t {
	case PARAM_UNCHECKED:
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

func ParseJob(jobdesc string) (*Job, error) {
	parts := strings.Split(jobdesc, " ")
	if len(parts) == 0 {
		return nil, errors.New("Empty job description")
	}

	ourJob := &Job{sourceDesc: jobdesc}

	found := false
	for _, c := range commands {
		if parts[0] == c.keyword {
			found = true

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
		return nil, errors.New("Invalid parameters for command!")
	}
	return nil, errors.New("Unknown command")
}
