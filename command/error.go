package command

import (
	"errors"
	"fmt"
	"strings"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
	"github.com/hashicorp/go-multierror"

	errorpkg "github.com/peak/s5cmd/v2/error"
	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/storage/url"
)

func printDebug(op string, err error, urls ...*url.URL) {
	var command strings.Builder
	command.WriteString(op)
	for _, url := range urls {
		if url != nil {
			command.WriteString(fmt.Sprintf(" %s", url))
		}
	}

	msg := log.DebugMessage{
		Command:   command.String(),
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

// apiErrorMessage renders an SDK error as "Code: message status code: N",
// dropping the operation name, request id and host id that the v2 SDK wraps
// around every API error. Those add nothing for someone reading a copy
// failure, and the host id alone is a 100 character base64 blob.
// Returns "" when err is not an API error.
func apiErrorMessage(err error) string {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(apiErr.ErrorCode())
	sb.WriteString(":")
	if msg := apiErr.ErrorMessage(); msg != "" {
		sb.WriteString(" ")
		sb.WriteString(msg)
	}

	if respErr, ok := errors.AsType[*awshttp.ResponseError](err); ok {
		fmt.Fprintf(&sb, " status code: %d", respErr.HTTPStatusCode())
	}

	return sb.String()
}

// cleanupError converts multiline messages into
// a single line.
func cleanupError(err error) string {
	msg := apiErrorMessage(err)
	if msg == "" {
		msg = err.Error()
	}

	s := strings.Replace(msg, "\n", " ", -1)
	s = strings.Replace(s, "\t", " ", -1)
	s = strings.Replace(s, "  ", " ", -1)
	s = strings.TrimSpace(s)
	return s
}
