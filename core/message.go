package core

import (
	"context"
	"fmt"

	"github.com/peak/s5cmd/flags"
)

var Stdout = make(chan message, 10000)

type logLevel int

const (
	levelVerbose logLevel = iota
	levelInfo
	levelSuccess
	levelWarning
	levelError
)

func (l logLevel) String() string {
	switch l {
	case levelSuccess:
		return "+"
	case levelError:
		return "ERROR"
	case levelWarning:
		return "WARNING"
	case levelInfo:
		return "#"
	case levelVerbose:
		return "VERBOSE"
	default:
		return "UNKNOWN"
	}
}

type message struct {
	job   string
	level logLevel
	s     string
	err   error
}

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
