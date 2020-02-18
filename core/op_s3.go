package core

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/stats"
	"github.com/peak/s5cmd/storage"
)

func S3Copy(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	src, dst := job.args[0], job.args[1]

	err := CheckConditionals(src, dst, wp, job.opts)
	if err != nil {
		return opType, err
	}

	client, err := wp.newClient()
	if err != nil {
		return opType, err
	}

	err = client.Copy(
		wp.ctx,
		dst.url,
		src.url,
		job.getStorageClass(),
	)

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = client.Delete(wp.ctx, src.url.Bucket, src.url)
	}

	return opType, err
}

func S3Delete(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	client, err := wp.newClient()
	if err != nil {
		return opType, err
	}

	src := job.args[0]

	err = client.Delete(wp.ctx, src.url.Bucket, src.url)
	return opType, err
}

func S3BatchDelete(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	src := job.args[0]

	var jobArgs []*JobArgument
	srcBucket := *src.Clone()

	maxArgs := storage.DeleteItemsMax

	initArgs := func() {
		jobArgs = make([]*JobArgument, 0, maxArgs+1)
		jobArgs = append(jobArgs, &srcBucket)
	}

	makeJob := func(item *storage.Object) *Job {
		var subJob *Job

		if jobArgs == nil {
			initArgs()
		}

		if (item.IsMarkerObject() || len(jobArgs) == maxArgs) && len(jobArgs) > 1 {
			subJob = job.MakeSubJob("batch-rm", op.BatchDeleteActual, jobArgs, opt.OptionList{})
			initArgs()
		}

		if item != nil {
			jobArgs = append(jobArgs, NewJobArgument(item.URL))
		}

		return subJob
	}

	err := wildOperation(src.url, wp, func(item *storage.Object) *Job {
		if item.IsDirectory {
			return nil
		}

		return makeJob(item)
	})

	return opType, err
}

func S3BatchDeleteActual(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	client, err := wp.newClient()
	if err != nil {
		return opType, err
	}

	src := job.args[0]

	deleteObjects := make([]*objurl.ObjectURL, len(job.args)-1)
	for i, a := range job.args {
		if i == 0 {
			continue
		}
		deleteObjects[i-1] = a.url
	}

	err = client.Delete(wp.ctx, src.url.Bucket, deleteObjects...)
	st := client.Stats()

	for key, stat := range st.Keys() {
		if stat.Success {
			job.out(shortOk, `Batch-delete %v`, key)
		} else {
			job.out(shortErr, `Batch-delete %v: %s`, key, stat.Message)
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

	src, dst := job.args[0], job.args[1]

	err := wildOperation(src.url, wp, func(item *storage.Object) *Job {
		if item.IsMarkerObject() || item.IsDirectory {
			return nil
		}

		arg1 := NewJobArgument(item.URL)

		var dstFn string
		if job.opts.Has(opt.Parents) {
			dstFn = item.URL.Path
		} else {
			dstFn = item.URL.Base()
		}

		arg2 := dst.Join(dstFn)

		subJob := job.MakeSubJob(subCmd, op.Download, []*JobArgument{arg1, arg2}, job.opts)

		if item.IsGlacierObject() {
			subJob.out(shortErr, `"%s": Cannot download glacier object`, arg1.url)
			return nil
		}
		dir := filepath.Dir(arg2.url.Absolute())
		os.MkdirAll(dir, os.ModePerm)
		return subJob
	})

	return opType, err
}

func S3Download(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	src, dst := job.args[0], job.args[1]

	err := CheckConditionals(src, dst, wp, job.opts)
	if err != nil {
		return opType, err
	}

	srcFn := src.url.Base()
	destFn := dst.url.Absolute()

	f, err := os.Create(destFn)
	if err != nil {
		return opType, err
	}

	client, err := wp.newClient()
	if err != nil {
		return opType, err
	}

	job.out(shortInfo, "Downloading %s...", srcFn)
	err = client.Get(wp.ctx, src.url, f)

	// FIXME(ig): i don't see a reason for a race condition if this call is
	// deferrred. Will check later.
	f.Close() // Race: s3dl.Download or us?

	if err != nil {
		os.Remove(destFn) // Remove partly downloaded file
	} else if job.opts.Has(opt.DeleteSource) {
		err = client.Delete(wp.ctx, src.url.Bucket, src.url)
	}

	return opType, err
}

func S3Upload(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	src, dst := job.args[0], job.args[1]

	if ex, err := src.Exists(wp); err != nil {
		return opType, err
	} else if !ex {
		return opType, os.ErrNotExist
	}

	err := CheckConditionals(src, dst, wp, job.opts)
	if err != nil {
		return opType, err
	}

	srcFn := src.url.Base()

	f, err := os.Open(src.url.Absolute())
	if err != nil {
		return opType, err
	}

	client, err := wp.newClient()
	if err != nil {
		return opType, err
	}

	defer f.Close()

	filesize, _ := src.Size(wp)
	job.out(shortInfo, "Uploading %s... (%d bytes)", srcFn, filesize)

	err = client.Put(
		wp.ctx,
		f,
		dst.url,
		job.getStorageClass(),
	)

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = os.Remove(src.url.Absolute())
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

	src, dst := job.args[0], job.args[1]

	err := wildOperation(src.url, wp, func(item *storage.Object) *Job {
		if item.IsMarkerObject() || item.IsDirectory {
			return nil
		}

		arg1 := NewJobArgument(item.URL)

		var dstFn string
		if job.opts.Has(opt.Parents) {
			dstFn = item.URL.Path
		} else {
			dstFn = item.URL.Base()
		}

		arg2s3path := fmt.Sprintf("s3://%v/%v%v", dst.url.Bucket, dst.url.Path, dstFn)
		arg2url, _ := objurl.New(arg2s3path)
		arg2 := NewJobArgument(arg2url)

		subJob := job.MakeSubJob(subCmd, op.Copy, []*JobArgument{arg1, arg2}, job.opts)
		if item.IsGlacierObject() {
			subJob.out(shortErr, `"%s": Cannot download glacier object`, arg1.url)
			return nil
		}
		return subJob
	})

	return opType, err
}

func S3ListBuckets(job *Job, wp *WorkerParams) (stats.StatType, error) {
	const opType = stats.S3Op

	client, err := wp.newClient()
	if err != nil {
		return opType, err
	}

	buckets, err := client.ListBuckets(wp.ctx, "")
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

	src := job.args[0]

	err := wildOperation(src.url, wp, func(item *storage.Object) *Job {
		if item.IsMarkerObject() {
			return nil
		}

		if item.IsDirectory {
			job.out(shortOk, "%19s %1s %-38s  %12s  %s", "", "", "", "DIR", item.URL.Path)
		} else {

			var cls, etag, size string

			switch item.StorageClass {
			case storage.ObjectStorageClassStandard:
				cls = ""
			case storage.ObjectStorageClassGlacier:
				cls = "G"
			case storage.ObjectStorageClassReducedRedundancy:
				cls = "R"
			case storage.TransitionStorageClassStandardIa:
				cls = "I"
			default:
				cls = "?"
			}

			if showETags {
				etag = strings.Trim(item.Etag, `"`)
			}
			if humanize {
				size = HumanizeBytes(item.Size)
			} else {
				size = fmt.Sprintf("%d", item.Size)
			}

			job.out(
				shortOk,
				"%s %1s %-38s %12s  %s",
				item.LastModified.Format(dateFormat),
				cls,
				etag,
				size,
				item.URL.Relative(),
			)
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

	src := job.args[0]

	totals := map[string]sizeAndCount{}
	err := wildOperation(src.url, wp, func(item *storage.Object) *Job {
		if item.IsMarkerObject() || item.IsDirectory {
			return nil
		}
		storageClass := item.StorageClass
		s := totals[storageClass]
		s.size += item.Size
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
				job.out(shortOk, "%s bytes in %d objects: %s [%s]", HumanizeBytes(v.size), v.count, src.url, k)
			} else {
				job.out(shortOk, "%d bytes in %d objects: %s [%s]", v.size, v.count, src.url, k)
			}
		}
	}
	return opType, err
}
