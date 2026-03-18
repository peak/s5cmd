package log

import (
	"fmt"
	"os"
	"sync"
)

// output is an internal container for messages to be logged.
type output struct {
	std     *os.File
	message string
}

// outputCh is used to synchronize writes to standard output. Multi-line
// logging is not possible if all workers print logs at the same time.
var outputCh = make(chan output, 10000)

var (
	global    *Logger
	closeOnce sync.Once
)

// Init inits global logger.
func Init(level string, json bool, opts ...LoggerOption) {
	closeOnce = sync.Once{}
	global = New(level, json, opts...)
}

// Trace prints message in trace mode.
func Trace(msg Message) {
	global.printf(LevelTrace, msg, global.stdout)
}

// Debug prints message in debug mode.
func Debug(msg Message) {
	global.printf(LevelDebug, msg, global.stdout)
}

// Info prints message in info mode.
func Info(msg Message) {
	global.printf(LevelInfo, msg, global.stdout)
}

// Stat prints stat message regardless of the log level with info print formatting.
// It uses printfHelper instead of printf to ignore the log level condition.
func Stat(msg Message) {
	global.printfHelper(LevelInfo, msg, global.stdout)
}

// Error prints message in error mode.
func Error(msg Message) {
	global.printf(LevelError, msg, global.stderr)
}

// Close closes logger and its channel. Safe to call multiple times.
func Close() {
	closeOnce.Do(func() {
		if global != nil {
			close(outputCh)
			<-global.donech

			if global.logFile != nil {
				global.logFile.Close()
			}
		}
	})
}

// LoggerOption configures the Logger.
type LoggerOption func(*Logger)

// WithLogFile sets the log output file. Both stdout and stderr messages
// are redirected to this file.
func WithLogFile(path string) LoggerOption {
	return func(l *Logger) {
		if path == "" {
			return
		}
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			return
		}
		l.stdout = f
		l.stderr = f
		l.logFile = f
	}
}

// Logger is a structure for logging messages.
type Logger struct {
	donech  chan struct{}
	json    bool
	level   LogLevel
	stdout  *os.File
	stderr  *os.File
	logFile *os.File // non-nil if logging to file; closed on Close()
}

// New creates new logger.
func New(level string, json bool, opts ...LoggerOption) *Logger {
	logLevel := LevelFromString(level)
	logger := &Logger{
		donech: make(chan struct{}),
		json:   json,
		level:  logLevel,
		stdout: os.Stdout,
		stderr: os.Stderr,
	}
	for _, opt := range opts {
		opt(logger)
	}
	go logger.out()
	return logger
}

// printf prints message according to the given level, message and std mode.
func (l *Logger) printf(level LogLevel, message Message, std *os.File) {
	if level < l.level {
		return
	}
	l.printfHelper(level, message, std)
}

func (l *Logger) printfHelper(level LogLevel, message Message, std *os.File) {
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

// LogLevel is the level of Logger.
type LogLevel int

const (
	LevelTrace LogLevel = iota
	LevelDebug
	LevelInfo
	LevelError
)

// String returns the string representation of logLevel.
func (l LogLevel) String() string {
	switch l {
	case LevelInfo:
		return ""
	case LevelError:
		return "ERROR "
	case LevelDebug:
		return "DEBUG "
	case LevelTrace:
		// levelTrace is used for printing aws sdk logs and
		// aws-sdk-go already adds "DEBUG" prefix to logs.
		// So do not add another prefix to log which makes it
		// look weird.
		return ""
	default:
		return "UNKNOWN "
	}
}

// LevelFromString returns logLevel for given string. It
// return `levelInfo` as a default.
func LevelFromString(s string) LogLevel {
	switch s {
	case "debug":
		return LevelDebug
	case "info":
		return LevelInfo
	case "error":
		return LevelError
	case "trace":
		return LevelTrace
	default:
		return LevelInfo
	}
}
