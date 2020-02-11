package core

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/s3url"
	"github.com/peak/s5cmd/stats"
	"github.com/peak/s5cmd/storage"
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

	err = wp.storage.Copy(
		wp.ctx,
		job.args[1].s3,
		job.args[0].s3,
		cls,
	)

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = wp.storage.Delete(wp.ctx, job.args[0].s3.Bucket, job.args[0].s3.Key)
	}

	return opType, err
}

func S3Delete(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	err := wp.storage.Delete(wp.ctx, job.args[0].s3.Bucket, job.args[0].s3.Key)
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

	addArg := func(item *storage.Item) *Job {
		var subJob *Job

		if jobArgs == nil {
			initArgs()
		}

		if (item == nil || len(jobArgs) == maxArgs) && len(jobArgs) > 1 {
			subJob = job.MakeSubJob("batch-rm", op.BatchDeleteActual, jobArgs, opt.OptionList{})
			initArgs()
		}

		if item != nil {
			jobArgs = append(jobArgs, &JobArgument{arg: item.Key})
		}

		return subJob
	}

	err := wildOperation(job.args[0].s3, wp, func(item *storage.Item) *Job {
		if item == nil {
			return addArg(nil)
		}

		if item.IsDirectory {
			return nil
		}

		return addArg(item)
	})

	return opType, err
}

func S3BatchDeleteActual(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	deleteObjects := make([]string, len(job.args)-1)
	for i, a := range job.args {
		if i == 0 {
			continue
		}
		deleteObjects[i-1] = a.arg
	}

	err := wp.storage.Delete(wp.ctx, job.args[0].s3.Bucket, deleteObjects...)
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

	err := wildOperation(job.args[0].s3, wp, func(item *storage.Item) *Job {
		if item == nil || item.IsDirectory {
			return nil
		}

		arg1 := NewJobArgument(
			"s3://"+job.args[0].s3.Bucket+"/"+item.Key,
			&s3url.S3Url{Bucket: job.args[0].s3.Bucket, Key: item.Key},
		)

		var dstFn string
		if job.opts.Has(opt.Parents) {
			dstFn = item.Key
		} else {
			dstFn = path.Base(item.Key)
		}

		arg2 := job.args[1].StripS3().Append(dstFn, true)
		subJob := job.MakeSubJob(subCmd, op.Download, []*JobArgument{arg1, arg2}, job.opts)

		if aws.StringValue(item.Content.StorageClass) == s3.ObjectStorageClassGlacier {
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
	err = wp.storage.Get(wp.ctx, job.args[0].s3, f)

	// FIXME(ig): i don't see a reason for a race condition if this call is
	// deferrred. Will check later.
	f.Close() // Race: s3dl.Download or us?

	if err != nil {
		os.Remove(destFn) // Remove partly downloaded file
	} else if job.opts.Has(opt.DeleteSource) {
		err = wp.storage.Delete(wp.ctx, job.args[0].s3.Bucket, job.args[0].s3.Key)
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
	job.out(shortInfo, "Uploading %s... (%d bytes)", srcFn, filesize)

	var cls string

	if job.opts.Has(opt.RR) {
		cls = s3.ObjectStorageClassReducedRedundancy
	} else if job.opts.Has(opt.IA) {
		cls = s3.TransitionStorageClassStandardIa
	} else {
		cls = s3.ObjectStorageClassStandard
	}

	err = wp.storage.Put(wp.ctx, f, job.args[1].s3, cls)
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

	err := wildOperation(job.args[0].s3, wp, func(item *storage.Item) *Job {
		if item == nil || item.IsDirectory {
			return nil
		}

		arg1 := NewJobArgument(
			"s3://"+job.args[0].s3.Bucket+"/"+item.Key,
			&s3url.S3Url{Bucket: job.args[0].s3.Bucket, Key: item.Key},
		)

		var dstFn string
		if job.opts.Has(opt.Parents) {
			dstFn = item.Key
		} else {
			dstFn = path.Base(item.Key)
		}

		arg2 := NewJobArgument(
			"s3://"+job.args[1].s3.Bucket+"/"+job.args[1].s3.Key+dstFn,
			&s3url.S3Url{Bucket: job.args[1].s3.Bucket, Key: job.args[1].s3.Key + dstFn},
		)

		subJob := job.MakeSubJob(subCmd, op.Copy, []*JobArgument{arg1, arg2}, job.opts)
		if aws.StringValue(item.Content.StorageClass) == s3.ObjectStorageClassGlacier {
			subJob.out(shortErr, `"%s": Cannot download glacier object`, arg1.arg)
			return nil
		}
		return subJob
	})

	return opType, err
}

func S3ListBuckets(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	buckets, err := wp.storage.ListBuckets(wp.ctx, "")
	if err == nil {
		for _, b := range buckets {
			job.out(shortOk, "%s  s3://%s", b.CreationDate.Format(dateFormat), b.Name)
		}
	}
	return opType, err

}

func S3List(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op
	showETags := job.opts.Has(opt.ListETags)
	humanize := job.opts.Has(opt.HumanReadable)
	err := wildOperation(job.args[0].s3, wp, func(item *storage.Item) *Job {
		if item == nil {
			return nil
		}

		if item.IsDirectory {
			job.out(shortOk, "%19s %1s %-38s  %12s  %s", "", "", "", "DIR", item.Key)
		} else {
			var (
				cls, etag, size string
			)

			switch aws.StringValue(item.Content.StorageClass) {
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
				etag = strings.Trim(*item.Content.ETag, `"`)
			}
			if humanize {
				size = HumanizeBytes(*item.Content.Size)
			} else {
				size = fmt.Sprintf("%d", *item.Content.Size)
			}

			job.out(shortOk, "%s %1s %-38s %12s  %s", item.Content.LastModified.Format(dateFormat), cls, etag, size, item.Key)
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
	err := wildOperation(job.args[0].s3, wp, func(item *storage.Item) *Job {
		if item == nil || item.IsDirectory {
			return nil
		}
		storageClass := aws.StringValue(item.Content.StorageClass)
		s := totals[storageClass]
		s.size += *item.Content.Size
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
