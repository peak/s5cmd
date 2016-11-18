package s5cmd

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/termie/go-shutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

type JobArgument struct {
	arg string
	s3  *s3url
}

type Job struct {
	sourceDesc     string // Source job description which we parsed this from
	command        string // Different from operation, as multiple commands can map to the same op
	operation      Operation
	args           []*JobArgument
	successCommand *Job
	failCommand    *Job
}

const DATE_FORMAT string = "2006/01/02 15:04:05"

func (j Job) String() (s string) {
	s = j.command
	for _, a := range j.args {
		s += " " + a.arg
	}
	//s += " # from " + j.sourceDesc
	return
}

func s3copy(svc *s3.S3, src, dst *s3url) error {
	_, err := svc.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(dst.bucket),
		Key:        aws.String(dst.key),
		CopySource: aws.String(src.format()),
	})
	return err
}

func s3delete(svc *s3.S3, obj *s3url) error {
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(obj.bucket),
		Key:    aws.String(obj.key),
	})
	return err
}

func s3list(ctx context.Context, svc *s3.S3, bucket, prefix string, useDelimiter bool, filter string) error {
	inp := s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	if useDelimiter {
		inp.SetDelimiter("/")
	}

	var (
		r   *regexp.Regexp
		err error
	)

	if filter != "" {
		filterRegex := regexp.QuoteMeta(filter)
		filterRegex = strings.Replace(filterRegex, "\\?", ".", -1)
		filterRegex = strings.Replace(filterRegex, "\\*", ".*?", -1)
		r, err = regexp.Compile(filterRegex)
		if err != nil {
			return err
		}
	}

	trimPrefix := filepath.Dir(prefix) + "/"
	if trimPrefix == "./" {
		trimPrefix = ""
	}

	var mu sync.Mutex
	canceled := false

	err = svc.ListObjectsV2Pages(&inp, func(p *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, c := range p.CommonPrefixes {
			key := *c.Prefix
			if strings.Index(key, trimPrefix) == 0 {
				key = key[len(trimPrefix):]
			}
			out("+", "%19s  %8s  %s", "", "DIR", key)
		}
		for _, c := range p.Contents {
			key := *c.Key
			if strings.Index(key, trimPrefix) == 0 {
				key = key[len(trimPrefix):]
			}
			if r != nil && !r.MatchString(key) {
				continue
			}
			out("+", "%s  %8d  %s", c.LastModified.Format(DATE_FORMAT), *c.Size, key)
		}

		select {
		case <-ctx.Done():
			mu.Lock()
			defer mu.Unlock()
			canceled = true
			return false
		default:
			return true
		}
	})

	if err == nil && canceled {
		return errors.New("Operation interrupted")
	}
	return err
}

func out(shortCode, format string, a ...interface{}) {
	s := fmt.Sprintf(format, a...)
	fmt.Println("                   ", shortCode, s)
}

func (j *Job) Run(wp *WorkerParams) error {
	//log.Printf("Running %v", j)

	switch j.operation {

	// Local operations
	case OP_LOCAL_DELETE:
		return wp.stats.IncrementIfSuccess(STATS_FILEOP, os.Remove(j.args[0].arg))

	case OP_LOCAL_MOVE:
		return wp.stats.IncrementIfSuccess(STATS_FILEOP, os.Rename(j.args[0].arg, j.args[1].arg))

	case OP_LOCAL_COPY:
		_, err := shutil.Copy(j.args[0].arg, j.args[1].arg, true)
		wp.stats.IncrementIfSuccess(STATS_FILEOP, err)
		return err

	case OP_SHELL_EXEC:
		strArgs := make([]string, 0)

		for i, a := range j.args {
			if i == 0 {
				continue
			}
			strArgs = append(strArgs, a.arg)
		}
		cmd := exec.CommandContext(wp.ctx, j.args[0].arg, strArgs...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return wp.stats.IncrementIfSuccess(STATS_SHELLOP, cmd.Run())

	// S3 operations
	case OP_COPY:
		return wp.stats.IncrementIfSuccess(STATS_S3OP, s3copy(wp.s3svc, j.args[0].s3, j.args[1].s3))

	case OP_MOVE:
		err := wp.stats.IncrementIfSuccess(STATS_S3OP, s3copy(wp.s3svc, j.args[0].s3, j.args[1].s3))
		if err == nil {
			err = s3delete(wp.s3svc, j.args[0].s3)
			// FIXME if err != nil try to rollback by deleting j.args[1].s3 ? What if we don't have permission to delete?
		}

		return err

	case OP_DELETE:
		return wp.stats.IncrementIfSuccess(STATS_S3OP, s3delete(wp.s3svc, j.args[0].s3))

	case OP_DOWNLOAD:
		dest_fn := filepath.Base(j.args[0].arg)
		if len(j.args) > 1 {
			dest_fn = j.args[1].arg
		}

		f, err := os.Create(dest_fn)
		if err != nil {
			return err
		}

		_, err = wp.s3dl.Download(f, &s3.GetObjectInput{
			Bucket: aws.String(j.args[0].s3.bucket),
			Key:    aws.String(j.args[0].s3.key),
		})

		f.Close()
		wp.stats.IncrementIfSuccess(STATS_S3OP, err)
		if err != nil {
			os.Remove(dest_fn) // Remove partly downloaded file
		}

		return err

	case OP_UPLOAD:
		f, err := os.Open(j.args[0].arg)
		if err != nil {
			return err
		}

		defer f.Close()
		_, err = wp.s3ul.Upload(&s3manager.UploadInput{
			Bucket: aws.String(j.args[1].s3.bucket),
			Key:    aws.String(j.args[1].s3.key),
			Body:   f,
		})
		wp.stats.IncrementIfSuccess(STATS_S3OP, err)
		return err

	case OP_LISTBUCKETS:
		o, err := wp.s3svc.ListBuckets(&s3.ListBucketsInput{})
		if err == nil {
			for _, b := range o.Buckets {
				out("+", "%s  s3://%s", b.CreationDate.Format(DATE_FORMAT), *b.Name)
			}
		}
		return wp.stats.IncrementIfSuccess(STATS_S3OP, err)

	case OP_LIST:
		prefix := strings.TrimRight(j.args[0].s3.key, "/")
		if prefix != "" {
			prefix += "/"
		}

		err := s3list(wp.ctx, wp.s3svc, j.args[0].s3.bucket, prefix, true, "")
		return wp.stats.IncrementIfSuccess(STATS_S3OP, err)

	case OP_LISTWILD:
		wildkey := j.args[0].s3.key
		loc := strings.IndexAny(wildkey, "?*")
		if loc == -1 {
			return errors.New("Wildcard parse error")
		}
		prefix := wildkey[:loc]
		filter := wildkey[loc+1:]

		err := s3list(wp.ctx, wp.s3svc, j.args[0].s3.bucket, prefix, false, filter)
		return wp.stats.IncrementIfSuccess(STATS_S3OP, err)

	case OP_ABORT:
		var (
			exitCode int64 = -1
			err      error
		)

		if len(j.args) > 0 {
			exitCode, err = strconv.ParseInt(j.args[0].arg, 10, 8)
			if err != nil {
				exitCode = 255
			}
		}

		ef := wp.ctx.Value("exitFunc").(func(int))
		ef(int(exitCode))

		return nil

	// Unhandled
	default:
		return fmt.Errorf("Unhandled operation %v", j.operation)
	}

}
