package core

import (
	"os"
	"os/exec"
	"strconv"
)

func ShellExec(job *Job, wp *WorkerParams) *JobResponse {
	src := job.src[0]
	var srcCmd, dstCmd string

	if src != nil {
		srcCmd = src.Absolute()
	}

	if job.dst != nil {
		dstCmd = job.dst.Absolute()
	}

	cmd := exec.CommandContext(wp.ctx, srcCmd, dstCmd)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()

	return jobResponse(err)
}

func ShellAbort(job *Job, wp *WorkerParams) *JobResponse {
	var (
		exitCode int64 = -1
		err      error
		src      = job.src[0]
	)

	if job.src != nil {
		exitCode, err = strconv.ParseInt(src.Absolute(), 10, 8)
		if err != nil {
			exitCode = 255
		}
	}

	exitFn := wp.ctx.Value(ExitFuncKey).(func(int))
	exitFn(int(exitCode))

	return jobResponse(nil)
}
