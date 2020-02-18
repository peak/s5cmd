package core

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/s3url"
	"github.com/peak/s5cmd/stats"
	"github.com/peak/s5cmd/storage"
)

func S3Copy(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	response := job.args[1].CheckConditions(wp, job.args[0], job.opts)
	if response != nil {
		return opType, response
	}

	client, err := wp.newClient()
	if err != nil {
		return opType, &JobResponse{status: statusErr, err: err}
	}

	err = client.Copy(
		wp.ctx,
		job.args[1].s3,
		job.args[0].s3,
		job.getStorageClass(),
	)

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = client.Delete(wp.ctx, job.args[0].s3.Bucket, job.args[0].s3.Key)
	}

	return opType, jobResponse(err)
}

func S3Delete(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	client, err := wp.newClient()
	if err != nil {
		return opType, &JobResponse{status: statusErr, err: err}
	}

	err = client.Delete(wp.ctx, job.args[0].s3.Bucket, job.args[0].s3.Key)
	return opType, jobResponse(err)
}

func S3BatchDelete(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	var jobArgs []*JobArgument
	srcBucket := *job.args[0].Clone()
	srcBucket.arg = "s3://" + srcBucket.s3.Bucket

	maxArgs := storage.DeleteItemsMax

	initArgs := func() {
		jobArgs = make([]*JobArgument, 0, maxArgs+1)
		jobArgs = append(jobArgs, &srcBucket)
	}

	addArg := func(item *storage.Item) *Job {
		var subJob *Job

		if jobArgs == nil {
			initArgs()
		}

		if (item.IsMarkerObject() || len(jobArgs) == maxArgs) && len(jobArgs) > 1 {
			subJob = job.MakeSubJob("batch-rm", op.BatchDeleteActual, jobArgs, opt.OptionList{})
			initArgs()
		}

		if item != nil {
			jobArgs = append(jobArgs, &JobArgument{arg: item.Key})
		}

		return subJob
	}

	client, err := wp.newClient()
	if err != nil {
		return opType, &JobResponse{status: statusErr, err: err}
	}

	err = wildOperation(client, job.args[0].s3, wp, func(item *storage.Item) *Job {
		if item.IsDirectory {
			return nil
		}

		return addArg(item)
	})

	return opType, jobResponse(err)
}

func S3BatchDeleteActual(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	client, err := wp.newClient()
	if err != nil {
		return opType, &JobResponse{status: statusErr, err: err}
	}

	deleteObjects := make([]string, len(job.args)-1)
	for i, a := range job.args {
		if i == 0 {
			continue
		}
		deleteObjects[i-1] = a.arg
	}

	err = client.Delete(wp.ctx, job.args[0].s3.Bucket, deleteObjects...)
	if err != nil {
		return opType, &JobResponse{status: statusErr, err: err}
	}

	st := client.Stats()

	var msg []string
	for key, stat := range st.Keys() {
		if stat.Success {
			msg = append(msg, fmt.Sprintf("Batch-delete s3://%s/%s", job.args[0].s3.Bucket, key))
		} else {
			msg = append(msg, fmt.Sprintf(`Batch-delete s3://%s/%s: %s`, job.args[0].s3.Bucket, key, stat.Message))
		}
	}

	return opType, &JobResponse{status: statusSuccess, message: msg}
}

func S3BatchDownload(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	subCmd := "cp"
	if job.operation == op.AliasBatchGet {
		subCmd = "get"
	}

	if job.opts.Has(opt.DeleteSource) {
		subCmd = "mv"
	}
	subCmd += job.opts.GetParams()

	client, err := wp.newClient()
	if err != nil {
		return opType, &JobResponse{status: statusErr, err: err}
	}

	err = wildOperation(client, job.args[0].s3, wp, func(item *storage.Item) *Job {
		if item.IsMarkerObject() || item.IsGlacierObject() || item.IsDirectory {
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
		dir := filepath.Dir(arg2.arg)
		os.MkdirAll(dir, os.ModePerm)
		return subJob
	})

	return opType, jobResponse(err)
}

func S3Download(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	response := job.args[1].CheckConditions(wp, job.args[0], job.opts)
	if response != nil {
		return opType, response
	}

	srcFn := path.Base(job.args[0].arg)
	destFn := job.args[1].arg

	f, err := os.Create(destFn)
	if err != nil {
		return opType, &JobResponse{status: statusErr, err: err}
	}

	client, err := wp.newClient()
	if err != nil {
		return opType, &JobResponse{status: statusErr, err: err}
	}

	infoLog("Downloading %s...", srcFn)
	err = client.Get(wp.ctx, job.args[0].s3, f)

	// FIXME(ig): i don't see a reason for a race condition if this call is
	// deferrred. Will check later.
	f.Close() // Race: s3dl.Download or us?

	if err != nil {
		os.Remove(destFn) // Remove partly downloaded file
	} else if job.opts.Has(opt.DeleteSource) {
		err = client.Delete(wp.ctx, job.args[0].s3.Bucket, job.args[0].s3.Key)
	}

	return opType, jobResponse(err)
}

func S3Upload(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	if ex, err := job.args[0].Exists(wp); err != nil {
		return opType, &JobResponse{status: statusErr, err: err}
	} else if !ex {
		return opType, &JobResponse{status: statusErr, err: os.ErrNotExist}
	}

	response := job.args[1].CheckConditions(wp, job.args[0], job.opts)
	if response != nil {
		return opType, response
	}

	srcFn := filepath.Base(job.args[0].arg)

	f, err := os.Open(job.args[0].arg)
	if err != nil {
		return opType, &JobResponse{status: statusErr, err: err}
	}

	client, err := wp.newClient()
	if err != nil {
		return opType, &JobResponse{status: statusErr, err: err}
	}

	defer f.Close()

	fileSize, err := job.args[0].Size(wp)
	if err != nil {
		return opType, &JobResponse{status: statusErr, err: err}
	}

	infoLog("Uploading %s... (%d bytes)", srcFn, fileSize)

	err = client.Put(
		wp.ctx,
		f,
		job.args[1].s3,
		job.getStorageClass(),
	)

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = os.Remove(job.args[0].arg)
	}

	return opType, jobResponse(err)
}

func S3BatchCopy(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	subCmd := "cp"
	if job.opts.Has(opt.DeleteSource) {
		subCmd = "mv"
	}
	subCmd += job.opts.GetParams()

	client, err := wp.newClient()
	if err != nil {
		return opType, &JobResponse{status: statusErr, err: err}
	}

	err = wildOperation(client, job.args[0].s3, wp, func(item *storage.Item) *Job {
		if item.IsMarkerObject() || item.IsGlacierObject() || item.IsDirectory {
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
		return subJob
	})

	return opType, jobResponse(err)
}

func S3ListBuckets(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	client, err := wp.newClient()
	if err != nil {
		return opType, &JobResponse{status: statusErr, err: err}
	}

	buckets, err := client.ListBuckets(wp.ctx, "")
	if err != nil {
		return opType, &JobResponse{status: statusErr, err: err}
	}

	var msg []string
	for _, b := range buckets {
		msg = append(msg, b.String())
	}

	return opType, &JobResponse{status: statusSuccess, message: msg}
}

func S3List(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op
	showETags := job.opts.Has(opt.ListETags)
	humanize := job.opts.Has(opt.HumanReadable)

	client, err := wp.newClient()
	if err != nil {
		return opType, &JobResponse{status: statusErr, err: err}
	}

	var msg []string
	for item := range client.List(wp.ctx, job.args[0].s3, storage.ListAllItems) {
		if item.IsMarkerObject() || item.Err != nil {
			continue
		}

		if item.IsDirectory {
			msg = append(msg, fmt.Sprintf("%19s %1s %-38s  %12s  %s", "", "", "", "DIR", item.Key))
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
			msg = append(msg, fmt.Sprintf("%s %1s %-38s %12s  %s", item.LastModified.Format(dateFormat), cls, etag, size, item.Key))
		}
	}

	return opType, &JobResponse{status: statusSuccess, message: msg}
}

func S3Size(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	type sizeAndCount struct {
		size  int64
		count int64
	}
	totals := map[string]sizeAndCount{}

	client, err := wp.newClient()
	if err != nil {
		return opType, &JobResponse{status: statusErr, err: err}
	}

	err = wildOperation(client, job.args[0].s3, wp, func(item *storage.Item) *Job {
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

	if err != nil {
		return opType, &JobResponse{status: statusErr, err: err}
	}

	sz := sizeAndCount{}
	if !job.opts.Has(opt.GroupByClass) {
		for k, v := range totals {
			sz.size += v.size
			sz.count += v.count
			delete(totals, k)
		}
		totals["Total"] = sz
	}

	var msg []string
	for k, v := range totals {
		if job.opts.Has(opt.HumanReadable) {
			msg = append(msg, fmt.Sprintf("%s bytes in %d objects: %s [%s]", HumanizeBytes(v.size), v.count, job.args[0].s3, k))
		} else {
			msg = append(msg, fmt.Sprintf("%d bytes in %d objects: %s [%s]", v.size, v.count, job.args[0].s3, k))
		}
	}

	return opType, &JobResponse{status: statusSuccess, message: msg}
}
