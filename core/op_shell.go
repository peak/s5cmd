package core

import (
	"context"
	"os"
	"os/exec"
	"strconv"
)

func ShellExec(ctx context.Context, job *Job) *JobResponse {
	// FIXME: SHELLEXEC
	strArgs := make([]string, 0)
	cmd := exec.CommandContext(ctx, job.src.Absolute(), strArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()

	return jobResponse(err)
}

func ShellAbort(ctx context.Context, job *Job) *JobResponse {
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

	exitFn := ctx.Value(ExitFuncKey).(func(int))
	exitFn(int(exitCode))

	return jobResponse(nil)
}
