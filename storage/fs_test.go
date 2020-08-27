package storage

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"gotest.tools/v3/assert"
)

func TestFilesystemImplementsStorageInterface(t *testing.T) {
	var i interface{} = new(Filesystem)
	if _, ok := i.(Storage); !ok {
		t.Errorf("expected %t to implement Storage interface", i)
	}
}

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
	}

	for _, tc := range testcases {
		tc := tc

		f, err := ioutil.TempFile("", tc.filename)
		if err != nil {
			t.Error(err)
		}

		if tc.content != "" {
			f.WriteString(tc.content)
			f.Seek(0, io.SeekStart)
		}

		file := &localFile{f}
		assert.Equal(t, tc.expectedContentType, file.ContentType())

		f.Close()
		os.Remove(f.Name())
	}
}
