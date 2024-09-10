package command

import (
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/hashicorp/go-multierror"

	errorpkg "github.com/peak/s5cmd/v2/error"
	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/storage/url"
)

func printDebug(op string, err error, urls ...*url.URL) {
	command := op
	for _, url := range urls {
		if url != nil {
			command += fmt.Sprintf(" %s", url)
		}
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

func handleMultipartError(command, op string, err error) error {
	var pkgErr *errorpkg.Error
	if err == nil {
		return err
	}

	if multiErr, ok := err.(*multierror.Error); ok {
		for _, merr := range multiErr.Errors {
			if errors.As(merr, &pkgErr) {
				if awsErr, ok := pkgErr.Err.(s3manager.MultiUploadFailure); ok {
					printError(command, op, fmt.Errorf("multipart upload fail. To resume use the following id: %s", awsErr.UploadID()))
				}
			}
		}
	} else {
		if errors.As(err, &pkgErr) {
			if awsErr, ok := pkgErr.Err.(s3manager.MultiUploadFailure); ok {
				printError(command, op, fmt.Errorf("multipart upload fail. To resume use the following id: %s", awsErr.UploadID()))
			}
		}
	}

	return err
}
