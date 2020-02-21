package core

import (
	"os"
	"os/exec"
	"strconv"
)

func ShellExec(job *Job, wp *WorkerParams) *JobResponse {
	strArgs := make([]string, 0)

	for _, src := range job.src {
		strArgs = append(strArgs, src.Absolute())
	}
	cmd := exec.CommandContext(wp.ctx, job.dst.Absolute(), strArgs...)
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
