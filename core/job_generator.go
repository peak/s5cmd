package core

import (
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/opt"
)

func S3BatchDownload(command *Command, sources ...*objurl.ObjectURL) *Job {
	cmd := "cp"
	if command.operation == op.AliasBatchGet {
		cmd = "get"
	}

	if command.opts.Has(opt.DeleteSource) {
		cmd = "mv"
	}

	cmd += command.opts.GetParams()
	cmdDst := command.dst
	src := sources[0]

	var dstFn string
	if command.opts.Has(opt.Parents) {
		dstFn = src.Path
	} else {
		dstFn = src.Base()
	}

	joinfn := filepath.Join
	if cmdDst.IsRemote() {
		joinfn = path.Join
	}

	dst := cmdDst.Clone()
	dst.Path = joinfn(dst.Path, dstFn)
	dir := filepath.Dir(dst.Absolute())
	os.MkdirAll(dir, os.ModePerm)
	return command.makeJob(cmd, op.Download, src, dst)
}

func S3BatchCopy(command *Command, sources ...*objurl.ObjectURL) *Job {
	cmd := "cp"
	if command.opts.Has(opt.DeleteSource) {
		cmd = "mv"
	}
	cmd += command.opts.GetParams()

	dst := command.dst
	src := sources[0]

	var dstFn string
	if command.opts.Has(opt.Parents) {
		dstFn = src.Path
	} else {
		dstFn = src.Base()
	}

	dstPath := fmt.Sprintf("s3://%v/%v%v", dst.Bucket, dst.Path, dstFn)
	dstUrl, _ := objurl.New(dstPath)
	return command.makeJob(cmd, op.Copy, src, dstUrl)
}

func S3BatchDelete(command *Command, sources ...*objurl.ObjectURL) *Job {
	return command.makeJob("batch-rm", op.BatchDeleteActual, nil, sources...)
}
