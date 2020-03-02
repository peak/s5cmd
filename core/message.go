package core

/*
import (
	"context"
	"fmt"

	"github.com/peak/s5cmd/flags"
)

func (m message) String() string {
	if m.level == levelSuccess || m.level == levelInfo {
		return fmt.Sprintf("                   %s %s", m.level, m.s)
	}

	errStr := ""
	if m.err != nil {
		if !*flags.Verbose && isCancelationError(m.err) {
			return ""
		}

		errStr = CleanupError(m.err)
		errStr = fmt.Sprintf(" (%s)", errStr)
	}

	if m.level == levelError {
		return fmt.Sprintf(`-ERR "%s": %s`, m.job, errStr)
	}

	return fmt.Sprintf(`%s "%s"%s`, m.level, m.job, errStr)
}

func sendMessage(ctx context.Context, msg message) {
	select {
	case <-ctx.Done():
	case Stdout <- msg:
	}
}

func newMessage(date, storageclass, etag, size, url string) message {
	return message{
		level: levelSuccess,
		s: fmt.Sprintf(
			"%19s %1s %-38s  %12s  %s",
			date,
			storageclass,
			etag,
			size,
			url,
		),
	}
}
*/
