// +build !windows

package fdlimit

import "syscall"

const (
	minOpenFilesLimit = 1024
)

func Raise() error {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return err
	}

	if rLimit.Cur >= minOpenFilesLimit {
		return nil
	}

	if rLimit.Max < minOpenFilesLimit {
		return nil
	}

	rLimit.Cur = minOpenFilesLimit

	return syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
}
