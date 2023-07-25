package e2e

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/icmd"
)

// pipe s3://bucket/object.zip
func TestUploadStdinToS3(t *testing.T) {
	t.Parallel()

	const (
		filename = "verylargefile.zip"
		content  = "Lorem ipsum dolor sit amet"
	)

	expectedContentType := "application/zip"

	if runtime.GOOS == "windows" {
		expectedContentType = "application/x-zip-compressed"
	}

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd := setup(t)

	createBucket(t, s3client, bucket)

	reader := bytes.NewBufferString(content)

	dstpath := fmt.Sprintf("s3://%v/%v", bucket, filename)

	cmd := s5cmd("pipe", dstpath)

	result := icmd.RunCmd(cmd, icmd.WithStdin(reader))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`pipe %v`, dstpath),
	})

	// assert that all was read from the fake stdin
	assert.Equal(t, 0, reader.Len())

	// assert S3
	assert.Assert(t, ensureS3Object(s3client, bucket, filename, content, ensureContentType(expectedContentType)))
}

// pipe s3://bucket/object
func TestUploadStdinToS3WithoutFileExtension(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	const (
		// make sure that Put reads the file header and guess Content-Type correctly.
		filename = "index"
		content  = `
<html lang="en">
	<head>
	<meta charset="utf-8">
	<body>
		<div id="foo">
			<div class="bar"></div>
		</div>
		<div id="baz">
			<style data-hey="naber"></style>
		</div>
	</body>
</html>
`
		expectedContentType = "application/octet-stream"
	)

	reader := bytes.NewBufferString(content)

	dstpath := fmt.Sprintf("s3://%v/%v", bucket, filename)

	cmd := s5cmd("pipe", dstpath)
	result := icmd.RunCmd(cmd, icmd.WithStdin(reader))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`pipe %v`, dstpath),
	})

	// assert S3
	assert.Assert(t, ensureS3Object(s3client, bucket, filename, content, ensureContentType(expectedContentType)))
}

func TestUploadStdinToS3WithNoSuchUploadRetryCount(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	const (
		filename = "example.txt"
		content  = "Some example text"
	)

	reader := bytes.NewBufferString(content)
	dstpath := fmt.Sprintf("s3://%v/%v", bucket, filename)

	cmd := s5cmd("pipe", "--no-such-upload-retry-count", "5", dstpath)
	result := icmd.RunCmd(cmd, icmd.WithStdin(reader))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`pipe %v`, dstpath),
	})

	// assert S3
	assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
}

// pipe --raw s3://bucket/object
func TestUploadStdinToS3WithRawMode(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	const (
		filename = "ex*mple.txt"
		content  = "Some *** text"
	)

	reader := bytes.NewBufferString(content)
	dstpath := fmt.Sprintf("s3://%v/%v", bucket, filename)

	cmd := s5cmd("pipe", "--raw", dstpath)
	result := icmd.RunCmd(cmd, icmd.WithStdin(reader))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`pipe %v`, dstpath),
	})

	// assert S3
	assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
}

// pipe s3://bucket/prefix/target
func TestUploadStdinToS3WithPrefix(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd := setup(t)

	const (
		filename = "testfile1.txt"
		content  = "this is the content"
	)

	createBucket(t, s3client, bucket)

	reader := bytes.NewBufferString(content)

	dstpath := fmt.Sprintf("s3://%v/s5cmdtest/%v", bucket, filename)

	cmd := s5cmd("pipe", dstpath)
	result := icmd.RunCmd(cmd, icmd.WithStdin(reader))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`pipe %v`, dstpath),
	})

	// assert s3 object
	assert.Assert(t, ensureS3Object(s3client, bucket, fmt.Sprintf("s5cmdtest/%v", filename), content))
}

// pipe -n s3://bucket/object
func TestUploadStdinToS3WithNoClobber(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd := setup(t)

	const (
		filename   = "test.txt"
		content    = "this text should be preserved"
		newContent = "new content should not be written"
	)

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, filename, content)

	dst := "s3://" + bucket + "/" + filename
	cmd := s5cmd("pipe", "-n", dst)
	reader := bytes.NewBufferString(content)
	result := icmd.RunCmd(cmd, icmd.WithStdin(reader))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{})

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	// expect s3 object is not overridden
	assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
}

// -log=debug pipe -n s3://bucket/object (bucket/file exists)
func TestUploadStdinToS3WithSameFilenameWithNoClobber(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd := setup(t)

	const (
		filename   = "testfile1.txt"
		content    = "this is the content"
		newContent = content + "\n"
	)

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, filename, content)

	reader := bytes.NewBufferString(newContent)

	dst := fmt.Sprintf("s3://%v/%v", bucket, filename)
	cmd := s5cmd("--log=debug", "pipe", "-n", dst)
	result := icmd.RunCmd(cmd, icmd.WithStdin(reader))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`DEBUG "pipe s3://%v/%v": object already exists`, bucket, filename),
	})

	assertLines(t, result.Stderr(), map[int]compareFunc{})

	// expect s3 object is not overridden
	assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
}

// pipe s3://bucket/object (bucket/file exists)
func TestUploadStdinToS3WithTheSameFilename(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd := setup(t)

	const (
		filename   = "testfile1.txt"
		content    = "this is the content"
		newContent = content + "\n"
	)

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, filename, content)

	reader := bytes.NewBufferString(newContent)

	dst := fmt.Sprintf("s3://%v/%v", bucket, filename)

	cmd := s5cmd("pipe", dst)
	result := icmd.RunCmd(cmd, icmd.WithStdin(reader))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(`pipe %v`, dst),
	})

	// expect s3 object to be updated with new content
	assert.Assert(t, ensureS3Object(s3client, bucket, filename, newContent))
}

func TestUploadStdinToS3WithAdjacentSlashes(t *testing.T) {
	t.Parallel()

	const (
		filename = "index.txt"
		content  = "this is a test file"
	)

	testcases := []struct {
		name          string
		dstpathprefix string
	}{
		{
			name:          "pipe s3://bucket//a/b/",
			dstpathprefix: "/a/b/",
		},
		{
			name:          "pipe s3://bucket/a//b/",
			dstpathprefix: "a//b/",
		},
		{
			name:          "pipe s3://bucket/a/b//",
			dstpathprefix: "a/b//",
		},
		{
			name:          "pipe s3://bucket//a///b/",
			dstpathprefix: "/a///b/",
		},
		{
			name:          "pipe s3://bucket/a//b///",
			dstpathprefix: "a//b///",
		},
		{
			name:          "pipe s3://bucket/a//b//c//d///",
			dstpathprefix: "a//b//c//d///",
		},
		{
			name:          "pipe s3://bucket/bar/s3://",
			dstpathprefix: "bar/s3://",
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			bucket := s3BucketFromTestName(t)

			s3client, s5cmd := setup(t)

			createBucket(t, s3client, bucket)

			reader := bytes.NewBufferString(content)
			objkey := fmt.Sprintf("%v/%v", tc.dstpathprefix, filename)
			dstpath := fmt.Sprintf("s3://%v/%v", bucket, objkey)
			cmd := s5cmd("pipe", dstpath)
			result := icmd.RunCmd(cmd, icmd.WithStdin(reader))
			result.Assert(t, icmd.Success)

			assertLines(t, result.Stdout(), map[int]compareFunc{
				0: suffix(`pipe %v`, dstpath),
			})

			// assert S3
			assert.Assert(t, ensureS3Object(s3client, bucket, objkey, content))
		})

	}

}

// --json pipe s3://bucket/object
func TestUploadStdinToS3JSON(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	const (
		filename = "testfile1.txt"
		content  = "this is a test file"
	)

	reader := bytes.NewBufferString(content)

	cmd := s5cmd("--json", "pipe", "s3://"+bucket+"/"+filename)
	result := icmd.RunCmd(cmd, icmd.WithStdin(reader))

	jsonText := `
		{
			"operation": "pipe",
			"success": true,
			"destination": "s3://%v/testfile1.txt",
			"object": {
				"type": "file"
			}
		}
	`

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: json(jsonText, bucket),
	}, jsonCheck(true))

	// assert S3
	assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
}

// pipe --storage-class=GLACIER s3://bucket/object
func TestUploadStdinToS3WithStorageClassGlacier(t *testing.T) {
	t.Parallel()

	// storage class GLACIER does not exist in GCS.
	skipTestIfGCS(t, "storage class GLACIER does not exist in GCS.")

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	const (
		// make sure that Put reads the file header, not the extension
		filename             = "index.txt"
		content              = "content"
		expectedStorageClass = "GLACIER"
	)

	reader := bytes.NewBufferString(content)

	dstpath := fmt.Sprintf("s3://%v/%v", bucket, filename)

	cmd := s5cmd("pipe", "--storage-class=GLACIER", dstpath)
	result := icmd.RunCmd(cmd, icmd.WithStdin(reader))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`pipe %v`, dstpath),
	})

	// assert S3
	assert.Assert(t, ensureS3Object(s3client, bucket, filename, content, ensureStorageClass(expectedStorageClass)))
}

// pipe --content-disposition inline s3://bucket/object
func TestUploadStdinToToS3WithContentDisposition(t *testing.T) {
	t.Parallel()

	s3client, s5cmd := setup(t)

	bucket := s3BucketFromTestName(t)
	createBucket(t, s3client, bucket)

	const (
		// make sure that Put reads the file header and guess Content-Type correctly.
		filename = "index.html"
		content  = `
<html lang="tr">
	<head>
	<meta charset="utf-8">
	<body>
		<header></header>
		<main></main>
		<footer></footer>
	</body>
</html>
`
		expectedContentType        = "text/html; charset=utf-8"
		expectedContentDisposition = "inline"
	)

	dstpath := fmt.Sprintf("s3://%v/%v", bucket, filename)
	reader := bytes.NewBufferString(content)

	cmd := s5cmd("pipe", "--content-disposition", "inline", dstpath)
	result := icmd.RunCmd(cmd, icmd.WithStdin(reader))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`pipe %v`, dstpath),
	})

	// assert S3
	assert.Assert(t, ensureS3Object(s3client, bucket, filename, content, ensureContentType(expectedContentType), ensureContentDisposition(expectedContentDisposition)))
}
