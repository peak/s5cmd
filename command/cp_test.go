package command

import (
	"context"
	"io"
	"os"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
)

// TestShouldOverride_NoFlagsIsNoop verifies that shouldOverride returns nil
// immediately when none of the override-gating flags are set. This confirms
// the fast-path remains intact and also serves as a compile-time check that
// the Copy struct fields referenced by shouldOverride (including dstRegion)
// are spelled correctly.
func TestShouldOverride_NoFlagsIsNoop(t *testing.T) {
	t.Parallel()

	srcURL, _ := url.New("s3://bucket/src.txt")
	dstURL, _ := url.New("s3://bucket/dst.txt")

	c := Copy{
		noClobber:     false,
		ifSizeDiffer:  false,
		ifSourceNewer: false,
		// dstRegion is set; the fix ensures it would be forwarded to dstOpts
		// when the function proceeds past the early return.
		dstRegion:   "us-west-2",
		storageOpts: storage.Options{},
	}

	// shouldOverride must return nil (no-op) when no override flag is set.
	if err := c.shouldOverride(context.Background(), srcURL, dstURL); err != nil {
		t.Errorf("expected nil when no override flags set, got %v", err)
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
