package command

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGuessContentType(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		filename string
		content  string

		expectedContentType string
	}{
		{
			filename:            "*.pdf",
			expectedContentType: "application/pdf",
		},
		{
			filename:            "*.css",
			expectedContentType: "text/css; charset=utf-8",
		},
		{
			filename: "index",
			content: `
					<!DOCTYPE html>
					<html>
						<head>
							<title>Hello World</title>
						</head>
						<body>
							<p>Hello, World! I am s5cmd :)</p>
						</body>
					</html>
					`,
			expectedContentType: "text/html; charset=utf-8",
		},
		// check file extension first without checking the content
		{
			filename: "index*.txt",
			content: `
					<!DOCTYPE html>
					<html>
						<head>
							<title>Hello World</title>
						</head>
						<body>
							<p>Hello, World! I am s5cmd :)</p>
						</body>
					</html>
					`,
			expectedContentType: "text/plain; charset=utf-8",
		},
	}

	for _, tc := range testcases {
		tc := tc

		f, err := os.CreateTemp("", tc.filename)
		if err != nil {
			t.Error(err)
		}

		if tc.content != "" {
			f.WriteString(tc.content)
			f.Seek(0, io.SeekStart)
		}

		assert.Equal(t, tc.expectedContentType, guessContentType(f))

		f.Close()
		os.Remove(f.Name())
	}
}
