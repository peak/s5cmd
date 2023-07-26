package command

import (
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
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

func TestShouldCopyObject(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		excludePatterns []string
		includePatterns []string
		objects         []string
		filteredObjects []string
	}{
		{
			excludePatterns: []string{"*.txt", "*.log"},
			includePatterns: []string{"file-*.doc"},
			objects:         []string{"document.txt", "file-2.log", "file-1.doc", "image.png"},
			filteredObjects: []string{"file-1.doc"},
		},
		{
			excludePatterns: []string{"secret-*"},
			includePatterns: []string{"*.txt", "*.log"},
			objects:         []string{"secret-passwords.txt", "file-1.txt", "file-2.txt", "image.png"},
			filteredObjects: []string{"file-1.txt", "file-2.txt"},
		},
		{
			excludePatterns: []string{},
			includePatterns: []string{"*.png"},
			objects:         []string{"secret-passwords.txt", "file-1.txt", "file-2.txt", "image.png"},
			filteredObjects: []string{"image.png"},
		},
		{
			excludePatterns: []string{"file*"},
			includePatterns: []string{},
			objects:         []string{"readme.md", "file-1.txt", "file-2.txt", "image.png"},
			filteredObjects: []string{"readme.md", "image.png"},
		},
		{
			excludePatterns: []string{"file*"},
			includePatterns: []string{"*txt"},
			objects:         []string{"readme.txt", "file-1.txt", "file-2.txt", "license.txt"},
			filteredObjects: []string{"readme.txt", "license.txt"},
		},
		{
			excludePatterns: []string{"*tmp", "*.txt"},
			includePatterns: []string{"*png", "*.doc*"},
			objects:         []string{"readme.txt", "license.txt", "cache.tmp", "image.png", "eula.doc", "eula.docx", "personaldoc"},
			filteredObjects: []string{"image.png", "eula.doc", "eula.docx"},
		},
	}

	for _, tc := range testcases {
		tc := tc
		excludeRegex, err := createRegexFromWildcard(tc.excludePatterns)
		if err != nil {
			t.Error(err)
		}
		includeRegex, err := createRegexFromWildcard(tc.includePatterns)
		if err != nil {
			t.Error(err)
		}
		var filteredObjects []string
		cp := Copy{excludePatterns: excludeRegex, includePatterns: includeRegex}
		cp.src = &url.URL{Prefix: ""}
		for _, object := range tc.objects {
			if cp.shouldCopyObject(&storage.Object{URL: &url.URL{Path: object}}, false) {
				filteredObjects = append(filteredObjects, object)
			}
		}
		assert.Equal(t, reflect.DeepEqual(tc.filteredObjects, filteredObjects), true)
	}
}
