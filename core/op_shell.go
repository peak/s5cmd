package core

import (
	"os"
	"os/exec"
	"strconv"
)

func ShellExec(job *Job, wp *WorkerParams) *JobResponse {
	strArgs := make([]string, 0)

	for i, a := range job.args {
		if i == 0 {
			continue
		}
		strArgs = append(strArgs, a.Absolute())
	}
	cmd := exec.CommandContext(wp.ctx, job.args[0].Absolute(), strArgs...)
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

	if len(job.args) > 0 {
		exitCode, err = strconv.ParseInt(job.args[0].Absolute(), 10, 8)
		if err != nil {
			exitCode = 255
		}
	}

	exitFn := wp.ctx.Value(ExitFuncKey).(func(int))
	exitFn(int(exitCode))

	return jobResponse(nil)
}
