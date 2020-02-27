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

func S3BatchDownload(command *Command, src *objurl.ObjectURL) *Job {
	cmd := "cp"
	if command.operation == op.AliasBatchGet {
		cmd = "get"
	}

	if command.opts.Has(opt.DeleteSource) {
		cmd = "mv"
	}

	cmd += command.opts.GetParams()
	cmdDst := command.args[1]

	var joinPath string
	if command.opts.Has(opt.Parents) {
		joinPath = src.Relative()
	} else {
		joinPath = src.Base()
	}

	dst := cmdDst.Join(joinPath)
	dir := filepath.Dir(dst.Absolute())

	os.MkdirAll(dir, os.ModePerm)
	return command.makeJob(cmd, op.Download, src, dst)
}

func S3BatchCopy(command *Command, src *objurl.ObjectURL) *Job {
	cmd := "cp"
	if command.opts.Has(opt.DeleteSource) {
		cmd = "mv"
	}
	cmd += command.opts.GetParams()

	dst := command.args[1]

	var dstFilename string
	if command.opts.Has(opt.Parents) {
		dstFilename = src.Relative()
	} else {
		dstFilename = src.Base()
	}

	dstPath := fmt.Sprintf("s3://%v/%v%v", dst.Bucket, dst.Path, dstFilename)
	dstUrl, _ := objurl.New(dstPath)
	return command.makeJob(cmd, op.Copy, src, dstUrl)
}

func BatchLocalCopy(command *Command, url *objurl.ObjectURL) *Job {
	return localCopy(command, op.LocalCopy, url)
}

func BatchLocalUpload(command *Command, url *objurl.ObjectURL) *Job {
	return localCopy(command, op.Upload, url)
}

func localCopy(command *Command, operation op.Operation, src *objurl.ObjectURL) *Job {
	cmd := "cp"
	if command.opts.Has(opt.DeleteSource) {
		cmd = "mv"
	}
	cmd += command.opts.GetParams()

	cmdSrc, cmdDst := command.args[0], command.args[1]

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
	return command.makeJob(cmd, operation, src, dst)
}
