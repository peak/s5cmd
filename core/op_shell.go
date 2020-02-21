package core

import (
	"os"
	"os/exec"
	"strconv"
)

func ShellExec(job *Job, wp *WorkerParams) *JobResponse {
	// FIXME: SHELLEXEC
	strArgs := make([]string, 0)
	cmd := exec.CommandContext(wp.ctx, job.src.Absolute(), strArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()

	return jobResponse(err)
}

func ShellAbort(job *Job, wp *WorkerParams) *JobResponse {
	var (
		exitCode int64 = -1
		err      error
	)

	if job.src != nil {
		exitCode, err = strconv.ParseInt(job.src.Absolute(), 10, 8)
		if err != nil {
			exitCode = 255
		}
	}

	exitFn := wp.ctx.Value(ExitFuncKey).(func(int))
	exitFn(int(exitCode))

	return jobResponse(nil)
}
