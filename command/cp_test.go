package command

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/peak/s5cmd/v2/storage"
	"gotest.tools/v3/assert"
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

func TestIsSourceNewer(t *testing.T) {
	t.Parallel()

	now := time.Now()
	earlier := now.Add(-time.Minute)

	testcases := []struct {
		name     string
		src      *time.Time
		dst      *time.Time
		expected bool
	}{
		{
			name:     "source newer than destination",
			src:      &now,
			dst:      &earlier,
			expected: true,
		},
		{
			name:     "source older than destination",
			src:      &earlier,
			dst:      &now,
			expected: false,
		},
		{
			name:     "same modification time",
			src:      &now,
			dst:      &now,
			expected: false,
		},
		{
			name:     "source modification time unavailable",
			src:      nil,
			dst:      &now,
			expected: true,
		},
		{
			name:     "destination modification time unavailable",
			src:      &now,
			dst:      nil,
			expected: true,
		},
		{
			name:     "both modification times unavailable",
			src:      nil,
			dst:      nil,
			expected: true,
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := &storage.Object{ModTime: tc.src}
			dst := &storage.Object{ModTime: tc.dst}

			got := isSourceNewer(src, dst)
			assert.Equal(t, got, tc.expected)
		})
	}
}
