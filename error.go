package s5cmd

import (
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
				if errCode == "InternalError" {
					return errCode, true
				}
				status := reqErr.StatusCode()
				if status == 500 {
					return "500", true
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
