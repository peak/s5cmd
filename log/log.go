package log

import (
	"fmt"
	"os"
)

// output is a structure for std logs.
type output struct {
	std     *os.File
	message string
}

// outputCh is used to synchronize writes to standard output. Multi-line
// logging is not possible if all workers print logs at the same time.
var outputCh = make(chan output, 10000)

var globalLogger *logger

// Init inits global logger.
func Init(level string, json bool) {
	globalLogger = newLogger(level, json)
}

// logLevel is the level of Logger.
type logLevel int

const (
	levelDebug logLevel = iota
	levelInfo
	levelWarning
	levelError
)

// String returns the string representation of logLevel.
func (l logLevel) String() string {
	switch l {
	case levelInfo:
		return ""
	case levelError:
		return "ERROR "
	case levelWarning:
		return "WARNING "
	case levelDebug:
		return "DEBUG "
	default:
		return "UNKNOWN "
	}
}

// levelFromString returns logLevel for given string. It
// return `levelInfo` as a default.
func levelFromString(s string) logLevel {
	switch s {
	case "debug":
		return levelDebug
	case "info":
		return levelInfo
	case "warning":
		return levelWarning
	case "error":
		return levelError
	default:
		return levelInfo
	}
}

// logger is a structure for logging messages.
type logger struct {
	donech chan struct{}
	json   bool
	level  logLevel
}

// New creates new logger.
func newLogger(level string, json bool) *logger {
	logLevel := levelFromString(level)
	logger := &logger{
		donech: make(chan struct{}),
		json:   json,
		level:  logLevel,
	}
	go logger.out()
	return logger
}

// printf prints message according to the given level, message and std mode.
func (l *logger) printf(level logLevel, message Message, std *os.File) {
	if level < l.level {
		return
	}

	if l.json {
		outputCh <- output{
			message: message.JSON(),
			std:     std,
		}
	} else {
		outputCh <- output{
			message: fmt.Sprintf("%v%v", level, message.String()),
			std:     std,
		}
	}
}

// Debug prints message in debug mode.
func Debug(msg Message) {
	globalLogger.printf(levelDebug, msg, os.Stdout)
}

// Info prints message in info mode.
func Info(msg Message) {
	globalLogger.printf(levelInfo, msg, os.Stdout)
}

// Warning prints message in warning mode.
func Warning(msg Message) {
	globalLogger.printf(levelWarning, msg, os.Stderr)
}

// Error prints message in error mode.
func Error(msg Message) {
	globalLogger.printf(levelError, msg, os.Stderr)
}

// out listens for outputCh and logs messages.
func (l *logger) out() {
	defer close(l.donech)

	for output := range outputCh {
		_, _ = fmt.Fprintln(output.std, output.message)
	}
}

// Close closes logger and its channel.
func Close() {
	close(outputCh)
	<-globalLogger.donech
}
