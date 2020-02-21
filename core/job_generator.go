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
	"github.com/peak/s5cmd/storage"
)

func S3BatchDownload(command *Command, objects ...*storage.Object) *Job {
	cmd := "cp"
	if command.operation == op.AliasBatchGet {
		cmd = "get"
	}

	if command.opts.Has(opt.DeleteSource) {
		cmd = "mv"
	}

	cmd += command.opts.GetParams()
	cmdDst := command.dst
	obj := objects[0]
	src := obj.URL

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
	return command.makeJob(cmd, op.Download, dst, src)
}

func S3BatchCopy(command *Command, objects ...*storage.Object) *Job {
	cmd := "cp"
	if command.opts.Has(opt.DeleteSource) {
		cmd = "mv"
	}
	cmd += command.opts.GetParams()

	dst := command.dst
	obj := objects[0]
	src := obj.URL

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

func S3BatchDelete(command *Command, objects ...*storage.Object) *Job {
	var src []*objurl.ObjectURL
	for _, o := range objects {
		src = append(src, o.URL)
	}
	return command.makeJob("batch-rm", op.BatchDeleteActual, nil, src...)
}

func BatchLocalUpload(command *Command, objects ...*storage.Object) *Job {
	cmd := "cp"
	if command.opts.Has(opt.DeleteSource) {
		cmd = "mv"
	}
	cmd += command.opts.GetParams()

	obj := objects[0]
	src, cmdDst := obj.URL, command.dst

	walkMode := obj.Type.IsDir()
	trimPrefix := src.Absolute()
	if !walkMode {
		loc := strings.IndexAny(trimPrefix, GlobCharacters)
		if loc < 0 {
			return nil
		}
		trimPrefix = trimPrefix[:loc]
	}
	trimPrefix = path.Dir(trimPrefix)
	if trimPrefix == "." {
		trimPrefix = ""
	} else {
		trimPrefix += string(filepath.Separator)
	}

	var dstFn string
	if command.opts.Has(opt.Parents) {
		dstFn = src.Absolute()
		if strings.Index(dstFn, trimPrefix) == 0 {
			dstFn = dstFn[len(trimPrefix):]
		}
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
	return command.makeJob(cmd, op.Upload, dst, src)
}

func BatchLocalCopy(command *Command, objects ...*storage.Object) *Job {
	cmd := "cp"
	if command.opts.Has(opt.DeleteSource) {
		cmd = "mv"
	}
	cmd += command.opts.GetParams()

	obj := objects[0]
	src, cmdDst := obj.URL, command.dst

	trimPrefix := src.Absolute()
	globStart := src.Absolute()

	walkMode := obj.Type.IsDir()
	if !walkMode {
		loc := strings.IndexAny(trimPrefix, GlobCharacters)
		if loc < 0 {
			return nil
		}
		trimPrefix = trimPrefix[:loc]
	} else {
		if !strings.HasSuffix(globStart, string(filepath.Separator)) {
			globStart += string(filepath.Separator)
		}
		globStart = globStart + "*"
	}
	trimPrefix = path.Dir(trimPrefix)
	if trimPrefix == "." {
		trimPrefix = ""
	} else {
		trimPrefix += string(filepath.Separator)
	}

	var dstFn string
	if command.opts.Has(opt.Parents) {
		dstFn = src.Absolute()
		if strings.Index(dstFn, trimPrefix) == 0 {
			dstFn = dstFn[len(trimPrefix):]
		}
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
	return command.makeJob(cmd, op.LocalCopy, dst, src)
}
