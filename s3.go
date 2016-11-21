package s5cmd

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

var ErrInterrupted = errors.New("Operation interrupted")

func s3copy(svc *s3.S3, src, dst *s3url) (*s3.CopyObjectOutput, error) {
	return svc.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(dst.bucket),
		Key:        aws.String(dst.key),
		CopySource: aws.String(src.format()),
	})
}

func s3delete(svc *s3.S3, obj *s3url) (*s3.DeleteObjectOutput, error) {
	return svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(obj.bucket),
		Key:    aws.String(obj.key),
	})
}

func s3head(svc *s3.S3, obj *s3url) (*s3.HeadObjectOutput, error) {
	return svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(obj.bucket),
		Key:    aws.String(obj.key),
	})
}

type s3listItem struct {
	parsedKey      string
	key            *string
	lastModified   *time.Time
	size           int64
	class          *string
	isCommonPrefix bool
}

func s3list(ctx context.Context, svc *s3.S3, s3url *s3url, emitChan chan<- interface{}) error {
	inp := s3.ListObjectsV2Input{
		Bucket: aws.String(s3url.bucket),
	}

	wildkey := s3url.key
	var prefix, filter string
	loc := strings.IndexAny(wildkey, "?*")
	if loc == -1 {
		// no wildcard operation
		inp.SetDelimiter("/")

		prefix = strings.TrimRight(s3url.key, "/")
		if prefix != "" {
			prefix += "/"
		}
	} else {
		// wildcard operation
		prefix = wildkey[:loc]
		filter = wildkey[loc+1:]
	}
	inp.SetPrefix(prefix)

	var (
		r   *regexp.Regexp
		err error
	)

	if filter != "" {
		filterRegex := regexp.QuoteMeta(filter)
		filterRegex = strings.Replace(filterRegex, "\\?", ".", -1)
		filterRegex = strings.Replace(filterRegex, "\\*", ".*?", -1)
		r, err = regexp.Compile(filterRegex + "$")
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
				isCommonPrefix: false,
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

func s3wildOperation(url *s3url, wp *WorkerParams, callback s3wildCallback) error {
	return wildOperation(wp, func(ch chan<- interface{}) error {
		return s3list(wp.ctx, wp.s3svc, url, ch)
	}, func(data interface{}) *Job {
		if data == nil {
			return callback(nil)
		}
		return callback(data.(*s3listItem))
	})
}
