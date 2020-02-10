package core

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/stats"
	"github.com/peak/s5cmd/s3url"
)

func S3Copy(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	err := job.args[1].CheckConditionals(wp, job.args[0], job.opts)
	if err != nil {
		return opType, err
	}

	var cls string

	if job.opts.Has(opt.RR) {
		cls = s3.ObjectStorageClassReducedRedundancy
	} else if job.opts.Has(opt.IA) {
		cls = s3.TransitionStorageClassStandardIa
	} else {
		cls = s3.ObjectStorageClassStandard
	}

	_, err = wp.s3svc.CopyObject(&s3.CopyObjectInput{
		Bucket:       aws.String(job.args[1].s3.Bucket),
		Key:          aws.String(job.args[1].s3.Key),
		CopySource:   aws.String(job.args[0].s3.Format()),
		StorageClass: aws.String(cls),
	})

	if job.opts.Has(opt.DeleteSource) && err == nil {
		_, err = s3delete(wp.s3svc, job.args[0].s3)
	}

	return opType, err
}

func S3Delete(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	_, err := s3delete(wp.s3svc, job.args[0].s3)
	return opType, err
}

func S3BatchDelete(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	var jobArgs []*JobArgument
	srcBucket := *job.args[0].Clone()
	srcBucket.arg = "s3://" + srcBucket.s3.Bucket

	maxArgs := 1000

	initArgs := func() {
		jobArgs = make([]*JobArgument, 0, maxArgs+1)
		jobArgs = append(jobArgs, &srcBucket)
	}

	addArg := func(key *string) *Job {
		var subJob *Job

		if jobArgs == nil {
			initArgs()
		}

		if (key == nil || len(jobArgs) == maxArgs) && len(jobArgs) > 1 {
			subJob = job.MakeSubJob("batch-rm", op.BatchDeleteActual, jobArgs, opt.OptionList{})
			initArgs()
		}

		if key != nil {
			jobArgs = append(jobArgs, &JobArgument{arg: *key})
		}

		return subJob
	}

	err := s3wildOperation(job.args[0].s3, wp, func(li *s3listItem) *Job {
		if li == nil {
			return addArg(nil)
		}

		if li.isDirectory {
			return nil
		}

		return addArg(li.Key)
	})

	return opType, err
}

func S3BatchDeleteActual(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	obj := make([]*s3.ObjectIdentifier, len(job.args)-1)
	for i, a := range job.args {
		if i == 0 {
			continue
		}
		obj[i-1] = &s3.ObjectIdentifier{Key: aws.String(a.arg)}
	}
	o, err := wp.s3svc.DeleteObjectsWithContext(wp.ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(job.args[0].s3.Bucket),
		Delete: &s3.Delete{
			Objects: obj,
		},
	})
	for _, o := range o.Deleted {
		job.out(shortOk, `Batch-delete s3://%s/%s`, job.args[0].s3.Bucket, *o.Key)
	}
	for _, e := range o.Errors {
		job.out(shortErr, `Batch-delete s3://%s/%s: %s`, job.args[0].s3.Bucket, *e.Key, *e.Message)
		if err != nil {
			err = errors.New(*e.Message)
		}
	}
	return opType, err

}

func S3BatchDownload(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	subCmd := "cp"
	if job.operation == op.AliasBatchGet {
		subCmd = "get"
	}

	if job.opts.Has(opt.DeleteSource) {
		subCmd = "mv"
	}
	subCmd += job.opts.GetParams()

	err := s3wildOperation(job.args[0].s3, wp, func(li *s3listItem) *Job {
		if li == nil || li.isDirectory {
			return nil
		}

		arg1 := NewJobArgument(
			"s3://"+job.args[0].s3.Bucket+"/"+*li.Key,
			&s3url.S3Url{Bucket: job.args[0].s3.Bucket, Key: *li.Key},
		)

		var dstFn string
		if job.opts.Has(opt.Parents) {
			dstFn = li.key
		} else {
			dstFn = path.Base(li.key)
		}

		arg2 := job.args[1].StripS3().Append(dstFn, true)
		subJob := job.MakeSubJob(subCmd, op.Download, []*JobArgument{arg1, arg2}, job.opts)

		if aws.StringValue(li.StorageClass) == s3.ObjectStorageClassGlacier {
			subJob.out(shortErr, `"%s": Cannot download glacier object`, arg1.arg)
			return nil
		}
		dir := filepath.Dir(arg2.arg)
		os.MkdirAll(dir, os.ModePerm)
		return subJob
	})

	return opType, err
}

func S3Download(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	err := job.args[1].CheckConditionals(wp, job.args[0], job.opts)
	if err != nil {
		return opType, err
	}

	srcFn := path.Base(job.args[0].arg)
	destFn := job.args[1].arg

	f, err := os.Create(destFn)
	if err != nil {
		return opType, err
	}

	job.out(shortInfo, "Downloading %s...", srcFn)

	ch := make(chan error, 1)

	go func() {
		var (
			err      error
			panicked bool
		)
		(func() {
			defer recoverer(ch, "s3manager.Download", &panicked)

			_, err = wp.s3dl.DownloadWithContext(wp.ctx, f, &s3.GetObjectInput{
				Bucket: aws.String(job.args[0].s3.Bucket),
				Key:    aws.String(job.args[0].s3.Key),
			}, func(u *s3manager.Downloader) {
				u.PartSize = wp.poolParams.DownloadChunkSizeBytes
				u.Concurrency = wp.poolParams.DownloadConcurrency
			})

		})()
		if !panicked {
			ch <- err
		}
		close(ch)
	}()

	select {
	case <-wp.ctx.Done():
		err = ErrInterrupted
	case err = <-ch:
		break
	}

	// FIXME(ig): i don't see a reason for a race condition if this call is
	// deferrred. Will check later.
	f.Close() // Race: s3dl.Download or us?

	if err != nil {
		os.Remove(destFn) // Remove partly downloaded file
	} else if job.opts.Has(opt.DeleteSource) {
		_, err = s3delete(wp.s3svc, job.args[0].s3)
	}

	return opType, err
}

func S3Upload(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	const bytesInMb = float64(1024 * 1024)

	if ex, err := job.args[0].Exists(wp); err != nil {
		return opType, err
	} else if !ex {
		return opType, os.ErrNotExist
	}

	err := job.args[1].CheckConditionals(wp, job.args[0], job.opts)
	if err != nil {
		return opType, err
	}

	srcFn := filepath.Base(job.args[0].arg)

	f, err := os.Open(job.args[0].arg)
	if err != nil {
		return opType, err
	}
	defer f.Close()

	filesize, _ := job.args[0].Size(wp)

	numPartsNeeded := filesize / wp.poolParams.UploadChunkSizeBytes
	chunkSize := int64(wp.poolParams.UploadChunkSizeBytes / int64(bytesInMb))
	if numPartsNeeded > s3manager.MaxUploadParts {
		cSize := float64(filesize / s3manager.MaxUploadParts)
		chunkSize = int64(math.Ceil(cSize / bytesInMb))
		job.out(shortInfo, "Uploading %s... (%d bytes) (chunk size %d MB)", srcFn, filesize, chunkSize)
	} else {
		job.out(shortInfo, "Uploading %s... (%d bytes)", srcFn, filesize)
	}

	ch := make(chan error, 1)

	go func(chunkSizeInBytes int64) {
		var cls string

		if job.opts.Has(opt.RR) {
			cls = s3.ObjectStorageClassReducedRedundancy
		} else if job.opts.Has(opt.IA) {
			cls = s3.TransitionStorageClassStandardIa
		} else {
			cls = s3.ObjectStorageClassStandard
		}

		var (
			err      error
			panicked bool
		)

		(func() {
			defer recoverer(ch, "s3manager.Upload", &panicked)

			_, err = wp.s3ul.UploadWithContext(wp.ctx, &s3manager.UploadInput{
				Bucket:       aws.String(job.args[1].s3.Bucket),
				Key:          aws.String(job.args[1].s3.Key),
				Body:         f,
				StorageClass: aws.String(cls),
			}, func(u *s3manager.Uploader) {
				u.PartSize = chunkSizeInBytes
				u.Concurrency = wp.poolParams.UploadConcurrency
			})
		})()
		if !panicked {
			ch <- err
		}
		close(ch)
	}(chunkSize * int64(bytesInMb))

	select {
	case <-wp.ctx.Done():
		err = ErrInterrupted
	case err = <-ch:
		break
	}

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = os.Remove(job.args[0].arg)
	}

	return opType, err
}

func S3BatchCopy(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	subCmd := "cp"
	if job.opts.Has(opt.DeleteSource) {
		subCmd = "mv"
	}
	subCmd += job.opts.GetParams()

	err := s3wildOperation(job.args[0].s3, wp, func(li *s3listItem) *Job {
		if li == nil || li.isDirectory {
			return nil
		}

		arg1 := NewJobArgument(
			"s3://"+job.args[0].s3.Bucket+"/"+*li.Key,
			&s3url.S3Url{Bucket: job.args[0].s3.Bucket, Key: *li.Key},
		)

		var dstFn string
		if job.opts.Has(opt.Parents) {
			dstFn = li.key
		} else {
			dstFn = path.Base(li.key)
		}

		arg2 := NewJobArgument(
			"s3://"+job.args[1].s3.Bucket+"/"+job.args[1].s3.Key+dstFn,
			&s3url.S3Url{Bucket: job.args[1].s3.Bucket, Key: job.args[1].s3.Key + dstFn},
		)

		subJob := job.MakeSubJob(subCmd, op.Copy, []*JobArgument{arg1, arg2}, job.opts)
		if aws.StringValue(li.StorageClass) == s3.ObjectStorageClassGlacier {
			subJob.out(shortErr, `"%s": Cannot download glacier object`, arg1.arg)
			return nil
		}
		return subJob
	})

	return opType, err
}

func S3ListBuckets(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	o, err := wp.s3svc.ListBucketsWithContext(wp.ctx, &s3.ListBucketsInput{})
	if err == nil {
		for _, b := range o.Buckets {
			job.out(shortOk, "%s  s3://%s", b.CreationDate.Format(dateFormat), *b.Name)
		}
	}
	return opType, err

}

func S3List(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	showETags := job.opts.Has(opt.ListETags)
	humanize := job.opts.Has(opt.HumanReadable)
	err := s3wildOperation(job.args[0].s3, wp, func(li *s3listItem) *Job {
		if li == nil {
			return nil
		}

		if li.isDirectory {
			job.out(shortOk, "%19s %1s %-38s  %12s  %s", "", "", "", "DIR", li.key)
		} else {
			var (
				cls, etag, size string
			)

			switch aws.StringValue(li.StorageClass) {
			case s3.ObjectStorageClassStandard:
				cls = ""
			case s3.ObjectStorageClassGlacier:
				cls = "G"
			case s3.ObjectStorageClassReducedRedundancy:
				cls = "R"
			case s3.TransitionStorageClassStandardIa:
				cls = "I"
			default:
				cls = "?"
			}

			if showETags {
				etag = strings.Trim(*li.ETag, `"`)
			}
			if humanize {
				size = HumanizeBytes(*li.Size)
			} else {
				size = fmt.Sprintf("%d", *li.Size)
			}

			job.out(shortOk, "%s %1s %-38s %12s  %s", li.LastModified.Format(dateFormat), cls, etag, size, li.key)
		}

		return nil
	})

	return opType, err
}

func S3Size(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	type sizeAndCount struct {
		size  int64
		count int64
	}
	totals := map[string]sizeAndCount{}
	err := s3wildOperation(job.args[0].s3, wp, func(li *s3listItem) *Job {
		if li == nil || li.isDirectory {
			return nil
		}
		storageClass := aws.StringValue(li.StorageClass)
		s := totals[storageClass]
		s.size += *li.Size
		s.count++
		totals[storageClass] = s

		return nil
	})
	if err == nil {
		sz := sizeAndCount{}
		if !job.opts.Has(opt.GroupByClass) {
			for k, v := range totals {
				sz.size += v.size
				sz.count += v.count
				delete(totals, k)
			}
			totals["Total"] = sz
		}

		for k, v := range totals {
			if job.opts.Has(opt.HumanReadable) {
				job.out(shortOk, "%s bytes in %d objects: %s [%s]", HumanizeBytes(v.size), v.count, job.args[0].s3, k)
			} else {
				job.out(shortOk, "%d bytes in %d objects: %s [%s]", v.size, v.count, job.args[0].s3, k)
			}
		}
	}
	return opType, err
}
