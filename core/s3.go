package core

import (
	"context"
	"errors"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/peakgames/s5cmd/url"
)

// ErrInterrupted is the error used when the main context is canceled
var ErrInterrupted = errors.New("Operation interrupted")

func s3delete(svc *s3.S3, obj *url.S3Url) (*s3.DeleteObjectOutput, error) {
	return svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(obj.Bucket),
		Key:    aws.String(obj.Key),
	})
}

func s3head(svc *s3.S3, obj *url.S3Url) (*s3.HeadObjectOutput, error) {
	return svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(obj.Bucket),
		Key:    aws.String(obj.Key),
	})
}

type s3listItem struct {
	parsedKey      string
	key            *string
	lastModified   *time.Time
	size           int64
	class          *string
	ETag           *string
	isCommonPrefix bool
}

func s3list(ctx context.Context, svc *s3.S3, s3url *url.S3Url, emitChan chan<- interface{}) error {
	inp := s3.ListObjectsV2Input{
		Bucket: aws.String(s3url.Bucket),
	}

	wildkey := s3url.Key
	var prefix, filter string
	loc := strings.IndexAny(wildkey, url.S3WildCharacters)
	wildOperation := loc > -1
	if !wildOperation {
		// no wildcard operation
		inp.SetDelimiter("/")
		prefix = s3url.Key

	} else {
		// wildcard operation
		prefix = wildkey[:loc]
		filter = wildkey[loc:]
	}
	inp.SetPrefix(prefix)

	var (
		r   *regexp.Regexp
		err error
	)

	trimPrefix := path.Dir(prefix) + "/"
	if trimPrefix == "./" {
		trimPrefix = ""
	}
	if !wildOperation {
		// prevent "ls s3://bucket/path/key" from matching s3://bucket/path/keyword
		// it will still match s3://bucket/path/keydir/ because we don't match the regex on commonPrefixes
		filter = prefix[len(trimPrefix):]
	}

	if filter != "" {
		filterRegex := regexp.QuoteMeta(filter)
		filterRegex = strings.Replace(filterRegex, "\\?", ".", -1)
		filterRegex = strings.Replace(filterRegex, "\\*", ".*?", -1)
		r, err = regexp.Compile("^" + filterRegex + "$")
		if err != nil {
			return err
		}
	}

	var mu sync.Mutex
	canceled := false
	isCanceled := func() bool {
		select {
		case <-ctx.Done():
			mu.Lock()
			defer mu.Unlock()
			canceled = true
			return true
		default:
			return false
		}
	}
	emit := func(item *s3listItem) bool {
		var data interface{}
		if item != nil {
			// avoid nil inside interface
			data = item
		}

		for {
			select {
			case <-ctx.Done():
				mu.Lock()
				defer mu.Unlock()
				canceled = true
				return false
			case emitChan <- data:
				return true
			}
		}
	}

	err = svc.ListObjectsV2Pages(&inp, func(p *s3.ListObjectsV2Output, lastPage bool) bool {
		if isCanceled() {
			return false
		}
		for _, c := range p.CommonPrefixes {
			key := *c.Prefix
			if strings.Index(key, trimPrefix) == 0 {
				key = key[len(trimPrefix):]
			}
			if !emit(&s3listItem{
				parsedKey:      key,
				key:            c.Prefix,
				isCommonPrefix: true,
			}) {
				return false
			}
		}
		for _, c := range p.Contents {
			key := *c.Key
			isCommonPrefix := wildOperation && key[len(key)-1] == '/' // Keys ending with prefix in wild output are "directories"
			if strings.Index(key, trimPrefix) == 0 {
				key = key[len(trimPrefix):]
			}
			if r != nil && !r.MatchString(key) {
				continue
			}
			if !emit(&s3listItem{
				parsedKey:      key,
				key:            c.Key,
				size:           *c.Size,
				lastModified:   c.LastModified,
				class:          c.StorageClass,
				ETag:           c.ETag,
				isCommonPrefix: isCommonPrefix,
			}) {
				return false
			}
		}
		if !*p.IsTruncated {
			emit(nil) // EOF
		}

		return !isCanceled()
	})

	mu.Lock()
	defer mu.Unlock()
	if err == nil && canceled {
		return ErrInterrupted
	}
	return err
}

type s3wildCallback func(*s3listItem) *Job

func s3wildOperation(url *url.S3Url, wp *WorkerParams, callback s3wildCallback) error {
	return wildOperation(wp, func(ch chan<- interface{}) error {
		return s3list(wp.ctx, wp.s3svc, url, ch)
	}, func(data interface{}) *Job {
		if data == nil {
			return callback(nil)
		}
		return callback(data.(*s3listItem))
	})
}
