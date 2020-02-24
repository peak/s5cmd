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

func S3Copy(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	var src, dst = job.args[0], job.args[1]

	response := CheckConditions(src, dst, wp, job.opts)
	if response != nil {
		return opType, response
	}

	client, err := wp.newClient(src.url)
	if err != nil {
		return opType, jobResponse(err)
	}

	err = client.Copy(
		wp.ctx,
		dst.url,
		src.url,
		job.getStorageClass(),
	)

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = client.Delete(wp.ctx, src.url)
	}

	return opType, jobResponse(err)
}

func S3Delete(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	src := job.args[0]

	client, err := wp.newClient(src.url)
	if err != nil {
		return opType, jobResponse(err)
	}

	err = client.Delete(wp.ctx, src.url)
	return opType, jobResponse(err)
}

func S3BatchDelete(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	src := job.args[0]

	var jobArgs []*JobArgument
	srcBucket := *src.Clone()

	maxArgs := storage.DeleteItemsMax

	initArgs := func() {
		jobArgs = make([]*JobArgument, 0, maxArgs+1)
		jobArgs = append(jobArgs, &srcBucket)
	}

	makeJob := func(object *storage.Object) *Job {
		var subJob *Job

		if jobArgs == nil {
			initArgs()
		}

		if (object.IsMarker() || len(jobArgs) == maxArgs) && len(jobArgs) > 1 {
			subJob = job.MakeSubJob("batch-rm", op.BatchDeleteActual, jobArgs, opt.OptionList{})
			initArgs()
		}

		if object != nil {
			jobArgs = append(jobArgs, NewJobArgument(object.URL))
		}

		return subJob
	}

	client, err := wp.newClient(src.url)
	if err != nil {
		return opType, jobResponse(err)
	}

	err = wildOperation(client, src.url, true, wp, func(object *storage.Object) *Job {
		if object.Mode.IsDir() {
			return nil
		}

		return makeJob(object)
	})

	return opType, jobResponse(err)
}

func S3BatchDeleteActual(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	src := job.args[0]

	client, err := wp.newClient(src.url)
	if err != nil {
		return opType, jobResponse(err)
	}

	deleteObjects := make([]*objurl.ObjectURL, len(job.args)-1)
	for i, a := range job.args {
		if i == 0 {
			continue
		}
		deleteObjects[i-1] = a.url
	}

	err = client.Delete(wp.ctx, deleteObjects...)
	if err != nil {
		return opType, jobResponse(err)
	}

	st := client.Statistics()

	var msg []string
	for key, stat := range st.Keys() {
		if stat.Success {
			msg = append(msg, fmt.Sprintf("Batch-delete %v", key))
		} else {
			msg = append(msg, fmt.Sprintf(`Batch-delete %v: %s`, key, stat.Message))
		}
	}

	return opType, jobResponse(err, msg...)
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

	src, dst := job.args[0], job.args[1]

	client, err := wp.newClient(src.url)
	if err != nil {
		return opType, jobResponse(err)
	}
	err = wildOperation(client, src.url, true, wp, func(object *storage.Object) *Job {
		if object.IsMarker() || object.Mode.IsDir() {
			return nil
		}

		arg1 := NewJobArgument(object.URL)

		var dstFn string
		if job.opts.Has(opt.Parents) {
			dstFn = object.URL.Path
		} else {
			dstFn = object.URL.Base()
		}

		arg2 := dst.Join(dstFn)

		subJob := job.MakeSubJob(subCmd, op.Download, []*JobArgument{arg1, arg2}, job.opts)

		dir := filepath.Dir(arg2.url.Absolute())
		os.MkdirAll(dir, os.ModePerm)
		return subJob
	})

	return opType, jobResponse(err)
}

func S3Download(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	src, dst := job.args[0], job.args[1]

	response := CheckConditions(src, dst, wp, job.opts)
	if response != nil {
		return opType, response
	}

	srcFn := src.url.Base()
	destFn := dst.url.Absolute()

	f, err := os.Create(destFn)
	if err != nil {
		return opType, jobResponse(err)
	}
	defer f.Close()

	// infer the client based on the source argument, which is a remote
	// storage.
	client, err := wp.newClient(src.url)
	if err != nil {
		return opType, jobResponse(err)
	}

	infoLog("Downloading %s...", srcFn)

	err = client.Get(wp.ctx, src.url, f)
	if err != nil {
		os.Remove(destFn) // Remove partly downloaded file
	} else if job.opts.Has(opt.DeleteSource) {
		err = client.Delete(wp.ctx, src.url)
	}

	return opType, jobResponse(err)
}

func S3Upload(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	src, dst := job.args[0], job.args[1]

	if ex, err := src.Exists(wp); err != nil {
		return opType, jobResponse(err)
	} else if !ex {
		return opType, jobResponse(os.ErrNotExist)
	}

	response := CheckConditions(src, dst, wp, job.opts)
	if response != nil {
		return opType, response
	}

	srcFn := src.url.Base()

	f, err := os.Open(src.url.Absolute())
	if err != nil {
		return opType, jobResponse(err)
	}
	defer f.Close()

	// infer the client based on destination, which is a remote storage.
	client, err := wp.newClient(dst.url)
	if err != nil {
		return opType, jobResponse(err)
	}

	fileSize, err := src.Size(wp)
	if err != nil {
		return opType, jobResponse(err)
	}

	infoLog("Uploading %s... (%d bytes)", srcFn, fileSize)

	metadata := map[string]string{
		"StorageClass": job.getStorageClass(),
		"ContentType":  "", // guess the mimetype (see: #33)
	}

	err = client.Put(
		wp.ctx,
		f,
		dst.url,
		metadata,
	)

	if job.opts.Has(opt.DeleteSource) && err == nil {
		err = os.Remove(src.url.Absolute())
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

	src, dst := job.args[0], job.args[1]

	client, err := wp.newClient(src.url)
	if err != nil {
		return opType, jobResponse(err)
	}

	err = wildOperation(client, src.url, true, wp, func(object *storage.Object) *Job {
		if object.IsMarker() || object.StorageClass.IsGlacier() || object.Mode.IsDir() {
			return nil
		}

		arg1 := NewJobArgument(object.URL)

		var dstFn string
		if job.opts.Has(opt.Parents) {
			dstFn = object.URL.Path
		} else {
			dstFn = object.URL.Base()
		}

		arg2s3path := fmt.Sprintf("s3://%v/%v%v", dst.url.Bucket, dst.url.Path, dstFn)
		arg2url, _ := objurl.New(arg2s3path)
		arg2 := NewJobArgument(arg2url)

		return job.MakeSubJob(subCmd, op.Copy, []*JobArgument{arg1, arg2}, job.opts)
	})

	return opType, jobResponse(err)
}

func S3ListBuckets(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	// set as remote storage
	url := &objurl.ObjectURL{Type: 0}
	client, err := wp.newClient(url)
	if err != nil {
		return opType, jobResponse(err)
	}

	buckets, err := client.ListBuckets(wp.ctx, "")
	if err != nil {
		return opType, jobResponse(err)
	}

	var msg []string
	for _, b := range buckets {
		msg = append(msg, b.String())
	}

	return opType, jobResponse(err, msg...)
}

func S3List(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	showETags := job.opts.Has(opt.ListETags)
	humanize := job.opts.Has(opt.HumanReadable)

	src := job.args[0]

	client, err := wp.newClient(src.url)
	if err != nil {
		return opType, jobResponse(err)
	}

	var msg []string
	for object := range client.List(wp.ctx, src.url, true, storage.ListAllItems) {
		if object.IsMarker() || object.Err != nil {
			continue
		}

		if object.Mode.IsDir() {
			msg = append(msg, fmt.Sprintf("%19s %1s %-38s  %12s  %s", "", "", "", "DIR", object.URL.Relative()))
		} else {
			var cls, etag, size string

			switch object.StorageClass {
			case storage.StorageStandard:
				cls = ""
			case storage.StorageGlacier:
				cls = "G"
			case storage.StorageReducedRedundancy:
				cls = "R"
			case storage.StorageStandardIA:
				cls = "I"
			default:
				cls = "?"
			}

			if showETags {
				etag = strings.Trim(object.Etag, `"`)
			}
			if humanize {
				size = HumanizeBytes(object.Size)
			} else {
				size = fmt.Sprintf("%d", object.Size)
			}

			msg = append(
				msg,
				fmt.Sprintf("%s %1s %-38s %12s  %s",
					object.ModTime.Format(dateFormat),
					cls,
					etag,
					size,
					object.URL.Relative(),
				),
			)
		}
	}

	return opType, jobResponse(err, msg...)
}

func S3Size(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.S3Op

	type sizeAndCount struct {
		size  int64
		count int64
	}

	src := job.args[0]

	totals := map[string]sizeAndCount{}

	client, err := wp.newClient(src.url)
	if err != nil {
		return opType, jobResponse(err)
	}

	err = wildOperation(client, src.url, true, wp, func(object *storage.Object) *Job {
		if object.IsMarker() || object.Mode.IsDir() {
			return nil
		}
		storageClass := string(object.StorageClass)
		s := totals[storageClass]
		s.size += object.Size
		s.count++
		totals[storageClass] = s

		return nil
	})

	if err != nil {
		return opType, jobResponse(err)
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
			msg = append(msg, fmt.Sprintf("%s bytes in %d objects: %s [%s]", HumanizeBytes(v.size), v.count, src.url, k))
		} else {
			msg = append(msg, fmt.Sprintf("%d bytes in %d objects: %s [%s]", v.size, v.count, src.url, k))
		}
	}

	return opType, jobResponse(err, msg...)
}
