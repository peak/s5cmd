package core

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/opt"
)

func S3BatchDownload(command *Command, urls ...*objurl.ObjectURL) *Job {
	cmd := "cp"
	if command.operation == op.AliasBatchGet {
		cmd = "get"
	}

	if command.opts.Has(opt.DeleteSource) {
		cmd = "mv"
	}

	cmd += command.opts.GetParams()
	cmdDst := command.dst
	src := urls[0]

	var joinPath string
	if command.opts.Has(opt.Parents) {
		joinPath = src.Path
	} else {
		joinPath = src.Base()
	}

	dst := cmdDst.Join(joinPath)
	dir := filepath.Dir(dst.Absolute())
	os.MkdirAll(dir, os.ModePerm)
	return command.makeJob(cmd, op.Download, dst, src)
}

func S3BatchCopy(command *Command, urls ...*objurl.ObjectURL) *Job {
	cmd := "cp"
	if command.opts.Has(opt.DeleteSource) {
		cmd = "mv"
	}
	cmd += command.opts.GetParams()

	dst := command.dst
	src := urls[0]

	var dstFn string
	if command.opts.Has(opt.Parents) {
		dstFn = src.Path
	} else {
		dstFn = src.Base()
	}

	dstPath := fmt.Sprintf("s3://%v/%v%v", dst.Bucket, dst.Path, dstFn)
	dstUrl, _ := objurl.New(dstPath)
	return command.makeJob(cmd, op.Copy, dstUrl, src)
}

func S3BatchDelete(command *Command, urls ...*objurl.ObjectURL) *Job {
	return command.makeJob("batch-rm", op.BatchDeleteActual, nil, urls...)
}

func BatchLocalCopy(command *Command, urls ...*objurl.ObjectURL) *Job {
	return localCopy(command, op.LocalCopy, urls...)
}

func BatchLocalUpload(command *Command, urls ...*objurl.ObjectURL) *Job {
	return localCopy(command, op.Upload, urls...)
}

func localCopy(command *Command, operation op.Operation, urls ...*objurl.ObjectURL) *Job {
	cmd := "cp"
	if command.opts.Has(opt.DeleteSource) {
		cmd = "mv"
	}
	cmd += command.opts.GetParams()

	cmdSrc, cmdDst := command.src, command.dst
	src := urls[0]

	trimPrefix := cmdSrc.Absolute()
	trimPrefix = path.Dir(trimPrefix)
	if trimPrefix == "." {
		trimPrefix = ""
	} else {
		trimPrefix += string(filepath.Separator)
	}

	var joinPath string
	if command.opts.Has(opt.Parents) {
		joinPath = src.Absolute()
		joinPath = strings.TrimPrefix(joinPath, trimPrefix)
	} else {
		joinPath = src.Base()
	}

	dst := cmdDst.Join(joinPath)
	if !dst.IsRemote() {
		dir := filepath.Dir(dst.Absolute())
		os.MkdirAll(dir, os.ModePerm)
	}
	return command.makeJob(cmd, operation, dst, src)
}
