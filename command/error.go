package command

import (
	"fmt"
	"strings"

	"github.com/hashicorp/go-multierror"

	errorpkg "github.com/peak/s5cmd/error"
	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/storage/url"
)

func printDebug(op string, err error, urls ...*url.URL) {
	command := op
	for _, url := range urls {
		command += fmt.Sprintf(" %s", url)
	}

	msg := log.DebugMessage{
		Command:   command,
		Operation: op,
		Err:       cleanupError(err),
	}
	log.Debug(msg)
}

// printError is the helper function to log error messages.
func printError(command, op string, err error) {
	// dont print cancelation errors
	if errorpkg.IsCancelation(err) {
		return
	}

	// check if we have our own error type
	{
		cerr, ok := err.(*errorpkg.Error)
		if ok {
			msg := log.ErrorMessage{
				Err:       cleanupError(cerr.Err),
				Command:   cerr.FullCommand(),
				Operation: cerr.Op,
			}
			log.Error(msg)
			return
		}
	}

	// check if errors are aggregated
	{
		merr, ok := err.(*multierror.Error)
		if ok {
			for _, err := range merr.Errors {
				customErr, ok := err.(*errorpkg.Error)
				if ok {
					msg := log.ErrorMessage{
						Err:       cleanupError(customErr.Err),
						Command:   customErr.FullCommand(),
						Operation: customErr.Op,
					}
					log.Error(msg)
					continue
				}

				msg := log.ErrorMessage{
					Err:       cleanupError(err),
					Command:   command,
					Operation: op,
				}

				log.Error(msg)
			}
			return
		}
	}

	// we don't know the exact error type. log the error as is.
	msg := log.ErrorMessage{
		Err:       cleanupError(err),
		Command:   command,
		Operation: op,
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
