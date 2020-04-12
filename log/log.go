package log

import (
	"fmt"
	"os"
)

// output is an internal container for messages to be logged.
type output struct {
	std     *os.File
	message string
}

// outputCh is used to synchronize writes to standard output. Multi-line
// logging is not possible if all workers print logs at the same time.
var outputCh = make(chan output, 10000)

var global *Logger

// Init inits global logger.
func Init(level string, json bool) {
	global = New(level, json)
}

// Debug prints message in debug mode.
func Debug(msg Message) {
	global.printf(levelDebug, msg, os.Stdout)
}

// Info prints message in info mode.
func Info(msg Message) {
	global.printf(levelInfo, msg, os.Stdout)
}

// Error prints message in error mode.
func Error(msg Message) {
	global.printf(levelError, msg, os.Stderr)
}

// Close closes logger and its channel.
func Close() {
	close(outputCh)
	<-global.donech
}

// Logger is a structure for logging messages.
type Logger struct {
	donech chan struct{}
	json   bool
	level  logLevel
}

// New creates new logger.
func New(level string, json bool) *Logger {
	logLevel := levelFromString(level)
	logger := &Logger{
		donech: make(chan struct{}),
		json:   json,
		level:  logLevel,
	}
	go logger.out()
	return logger
}

// printf prints message according to the given level, message and std mode.
func (l *Logger) printf(level logLevel, message Message, std *os.File) {
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

// out listens for outputCh and logs messages.
func (l *Logger) out() {
	defer close(l.donech)

	for output := range outputCh {
		_, _ = fmt.Fprintln(output.std, output.message)
	}
}

// logLevel is the level of Logger.
type logLevel int

const (
	levelDebug logLevel = iota
	levelInfo
	levelError
)

// String returns the string representation of logLevel.
func (l logLevel) String() string {
	switch l {
	case levelInfo:
		return ""
	case levelError:
		return "ERROR "
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
	case "error":
		return levelError
	default:
		return levelInfo
	}
}
