package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"strings"
)

func IsRetryableError(err error) (string, bool) {
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			//fmt.Println("awsErr", awsErr.Code(), awsErr.Message(), awsErr.OrigErr())

			errCode := awsErr.Code()
			switch errCode {
			case "SlowDown", "SerializationError":
				return errCode, true
			}

			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				//fmt.Println("reqErr", reqErr.StatusCode(), reqErr.RequestID())
				errCode = reqErr.Code()
				switch errCode {
				case "InternalError", "SerializationError":
					return errCode, true
				}
				status := reqErr.StatusCode()
				switch status {
				case 400, 500:
					return fmt.Sprintf("HTTP%d", status), true
				}
			}
		}
	}
	return "", false
}

func CleanupError(err error) (s string) {
	s = strings.Replace(err.Error(), "\n", " ", -1)
	s = strings.Replace(s, "\t", " ", -1)
	s = strings.Replace(s, "  ", " ", -1)
	s = strings.Replace(s, "  ", " ", -1)
	s = strings.TrimSpace(s)
	return
}
