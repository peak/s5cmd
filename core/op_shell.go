package core

import (
	"os"
	"os/exec"
	"strconv"

	"github.com/peak/s5cmd/stats"
)

func ShellExec(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.ShellOp

	strArgs := make([]string, 0)

	for i, a := range job.args {
		if i == 0 {
			continue
		}
		strArgs = append(strArgs, a.url.Absolute())
	}
	cmd := exec.CommandContext(wp.ctx, job.args[0].url.Absolute(), strArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()

	return opType, jobResponse(err)
}

func ShellAbort(job *Job, wp *WorkerParams) (stats.StatType, *JobResponse) {
	const opType = stats.ShellOp

	var (
		exitCode int64 = -1
		err      error
	)

	if len(job.args) > 0 {
		exitCode, err = strconv.ParseInt(job.args[0].url.Absolute(), 10, 8)
		if err != nil {
			exitCode = 255
		}
	}

	exitFn := wp.ctx.Value(ExitFuncKey).(func(int))
	exitFn(int(exitCode))

	return opType, jobResponse(nil)
}
