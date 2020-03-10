package command

import (
	"context"
	"errors"
	"strings"

	"github.com/hashicorp/go-multierror"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/storage"
)

func isCancelationError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) {
		return true
	}

	if storage.IsCancelationError(err) {
		return true
	}

	merr, ok := err.(*multierror.Error)
	if !ok {
		return false
	}

	for _, err := range merr.Errors {
		if isCancelationError(err) {
			return true
		}
	}

	return false
}

// printError is the helper function to log error messages.
func printError(command, operation string, err error) {
	msg := log.ErrorMessage{
		Err:       cleanupError(err),
		Command:   command,
		Operation: operation,
	}
	log.Error(msg)
}

// cleanupError converts multiline messages into
// a single line.
func cleanupError(err error) string {
	s := strings.Replace(err.Error(), "\n", " ", -1)
	s = strings.Replace(s, "\t", " ", -1)
	s = strings.Replace(s, "  ", " ", -1)
	s = strings.TrimSpace(s)
	return s
}
