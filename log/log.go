package log

import (
	"fmt"
	"log"
	"os"

	"github.com/peak/s5cmd/flags"
)

// stdoutCh is used to synchronize writes to standard output. Multi-line
// logging is not possible if all workers print logs at the same time.
var stdoutCh = make(chan string, 10000)

// Logger is the global logger.
var Logger *logger

// Init inits global logger.
func Init() {
	Logger = New()
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
		return "ERROR"
	case levelWarning:
		return "WARNING"
	case levelDebug:
		return "DEBUG"
	default:
		return "UNKNOWN"
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

// Message is an interface to print structured logs.
type Message interface {
	fmt.Stringer
	JSON() string
}

// logger is a structure for logging messages.
type logger struct {
	donech chan struct{}
	impl   *log.Logger
	level  logLevel
}

// New creates new logger.
func New() *logger {
	level := levelFromString(*flags.LogLevel)
	logger := &logger{
		donech: make(chan struct{}),
		impl:   log.New(os.Stdout, "", 0),
		level:  level,
	}
	go logger.stdout()
	return logger
}

// printf prints message according to the given level and message.
func (l *logger) printf(level logLevel, message Message) {
	if level < l.level {
		return
	}

	if *flags.JSON {
		msg := message.JSON()
		stdoutCh <- msg
	} else {
		msg := fmt.Sprintf("%v %v", level, message.String())
		stdoutCh <- msg
	}
}

// Debug prints message in debug mode.
func (l *logger) Debug(msg Message) {
	l.printf(levelDebug, msg)
}

// Info prints message in info mode.
func (l *logger) Info(msg Message) {
	l.printf(levelInfo, msg)
}

// Warning prints message in warning mode.
func (l *logger) Warning(msg Message) {
	l.printf(levelWarning, msg)
}

// Error prints message in error mode.
func (l *logger) Error(msg Message) {
	l.printf(levelError, msg)
}

// stdout listens for stdoutCh and logs messages.
func (l *logger) stdout() {
	defer close(l.donech)

	for msg := range stdoutCh {
		l.impl.Println(msg)
	}
}

// Close closes logger and its channel.
func (l *logger) Close() {
	close(stdoutCh)
	<-l.donech
}
