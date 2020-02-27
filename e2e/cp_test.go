package e2e

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/fs"
	"gotest.tools/v3/icmd"
)

func TestCopySingleS3ObjectToLocal(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	const (
		filename = "testfile1.txt"
		content  = "this is a file content"
	)

	putFile(t, s3client, bucket, filename, content)

	cmd := s5cmd("cp", "s3://"+bucket+"/"+filename, ".")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`# Downloading testfile1.txt...`),
	})

	// assert local filesystem
	expected := fs.Expected(t, fs.WithFile(filename, content, fs.WithMode(0644)))
	assert.Assert(t, fs.Equal(cmd.Dir, expected))

	// assert s3 object
	assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
}

func TestCopyMultipleFlatS3ObjectsToLocal(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	filesToContent := map[string]string{
		"testfile1.txt":          "this is a test file 1",
		"readme.md":              "this is a readme file",
		"filename-with-hypen.gz": "file has hypen in its name",
		"another_test_file.txt":  "yet another txt file. yatf.",
	}

	for filename, content := range filesToContent {
		putFile(t, s3client, bucket, filename, content)
	}

	cmd := s5cmd("cp", "s3://"+bucket+"/*", ".")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: suffix(`# Downloading another_test_file.txt...`),
		2: suffix(`# Downloading filename-with-hypen.gz...`),
		3: suffix(`# Downloading readme.md...`),
		4: suffix(`# Downloading testfile1.txt...`),
	}, sortInput(true))

	// assert local filesystem
	var expectedFiles []fs.PathOp
	for filename, content := range filesToContent {
		pathop := fs.WithFile(filename, content, fs.WithMode(0644))
		expectedFiles = append(expectedFiles, pathop)
	}
	expected := fs.Expected(t, expectedFiles...)
	assert.Assert(t, fs.Equal(cmd.Dir, expected))

	// assert s3 objects
	for filename, content := range filesToContent {
		assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
	}
}

func TestCopyMultipleNestedS3ObjectsToLocal(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	filesToContent := map[string]string{
		"testfile1.txt":              "this is a test file 1",
		"a/readme.md":                "this is a readme file",
		"a/b/filename-with-hypen.gz": "file has hypen in its name",
		// these 2 files are the same. we expect that only 1 file with this
		// filename reside in the working directory because 's5cmd cp
		// s3://.../* .' will flatten the folder hiearchy.
		"b/another_test_file.txt":     "yet another txt file. yatf.",
		"c/d/e/another_test_file.txt": "yet another txt file. yatf.",
	}

	for filename, content := range filesToContent {
		putFile(t, s3client, bucket, filename, content)
	}

	cmd := s5cmd("cp", "s3://"+bucket+"/*", ".")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: suffix(`# Downloading another_test_file.txt...`),
		2: suffix(`# Downloading another_test_file.txt...`),
		3: suffix(`# Downloading filename-with-hypen.gz...`),
		4: suffix(`# Downloading readme.md...`),
		5: suffix(`# Downloading testfile1.txt...`),
	}, sortInput(true))

	// assert local filesystem
	var expectedFiles []fs.PathOp
	for filename, content := range filesToContent {
		// trim nested folder structure because s5cmd will flatten the folder
		// hiearchy
		pathop := fs.WithFile(filepath.Base(filename), content, fs.WithMode(0644))
		expectedFiles = append(expectedFiles, pathop)
	}
	expected := fs.Expected(t, expectedFiles...)
	assert.Assert(t, fs.Equal(cmd.Dir, expected))

	// assert s3 objects
	for filename, content := range filesToContent {
		assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
	}
}

func TestCopyMultipleNestedS3ObjectsToLocalWithParents(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	filesToContent := map[string]string{
		"testfile1.txt":               "this is a test file 1",
		"a/readme.md":                 "this is a readme file",
		"a/b/filename-with-hypen.gz":  "file has hypen in its name",
		"b/another_test_file.txt":     "yet another txt file. yatf.",
		"c/d/e/another_test_file.txt": "yet another txt file. yatf.",
	}

	for filename, content := range filesToContent {
		putFile(t, s3client, bucket, filename, content)
	}

	cmd := s5cmd("cp", "--parents", "s3://"+bucket+"/*", ".")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: suffix(`# Downloading another_test_file.txt...`),
		2: suffix(`# Downloading another_test_file.txt...`),
		3: suffix(`# Downloading filename-with-hypen.gz...`),
		4: suffix(`# Downloading readme.md...`),
		5: suffix(`# Downloading testfile1.txt...`),
	}, sortInput(true))

	// assert local filesystem
	expected := fs.Expected(
		t,
		fs.WithFile("testfile1.txt", "this is a test file 1"),
		fs.WithDir(
			"a",
			fs.WithFile("readme.md", "this is a readme file"),
			fs.WithDir(
				"b",
				fs.WithFile("filename-with-hypen.gz", "file has hypen in its name"),
			),
		),
		fs.WithDir(
			"b",
			fs.WithFile("another_test_file.txt", "yet another txt file. yatf."),
		),
		fs.WithDir(
			"c",
			fs.WithDir(
				"d",
				fs.WithDir(
					"e",
					fs.WithFile("another_test_file.txt", "yet another txt file. yatf."),
				),
			),
		),
	)

	assert.Assert(t, fs.Equal(cmd.Dir, expected))

	// assert s3 objects
	for filename, content := range filesToContent {
		assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
	}
}

func TestCopyMultipleS3ObjectsToGivenLocalDirectory(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	filesToContent := map[string]string{
		"testfile1.txt":          "this is a test file 1",
		"readme.md":              "this is a readme file",
		"filename-with-hypen.gz": "file has hypen in its name",
		"another_test_file.txt":  "yet another txt file. yatf.",
	}

	for filename, content := range filesToContent {
		putFile(t, s3client, bucket, filename, content)
	}

	const dst = "given-directory"
	cmd := s5cmd("cp", "s3://"+bucket+"/*", dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: suffix(`# Downloading another_test_file.txt...`),
		2: suffix(`# Downloading filename-with-hypen.gz...`),
		3: suffix(`# Downloading readme.md...`),
		4: suffix(`# Downloading testfile1.txt...`),
	}, sortInput(true))

	// assert local filesystem
	var expectedFiles []fs.PathOp
	for filename, content := range filesToContent {
		pathop := fs.WithFile(filename, content, fs.WithMode(0644))
		expectedFiles = append(expectedFiles, pathop)
	}
	expected := fs.Expected(t, fs.WithDir(dst, expectedFiles...))
	assert.Assert(t, fs.Equal(cmd.Dir, expected))

	// assert s3 objects
	for filename, content := range filesToContent {
		assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
	}
}

func TestCopySingleFileToS3(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	const (
		filename = "testfile1.txt"
		content  = "this is a test file"
	)

	workdir := fs.NewDir(t, bucket, fs.WithFile(filename, content))
	defer workdir.Remove()

	fpath := workdir.Join(filename)

	cmd := s5cmd("cp", fpath, "s3://"+bucket+"/")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(` # Uploading %v...`, filename),
	})

	// assert local filesystem
	expected := fs.Expected(t, fs.WithFile(filename, content))
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// assert S3
	assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
}

func TestCopyDirToS3(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	folderLayout := []fs.PathOp{
		fs.WithFile("file1.txt", "this is the first test file"),
		fs.WithFile("readme.md", "this is a readme file"),
		fs.WithDir(
			"c",
			fs.WithFile("file2.txt", "this is the second test file"),
		),
	}

	workdir := fs.NewDir(t, t.Name(), folderLayout...)
	defer workdir.Remove()

	// this command ('s5cmd cp dir/ s3://bucket/') will run in 'walk' mode,
	// which is different than 'glob' mode.
	cmd := s5cmd("cp", workdir.Path()+"/", "s3://"+bucket+"/")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: contains(` # Uploading file1.txt...`),
		2: contains(` # Uploading file2.txt...`),
		3: contains(` # Uploading readme.md...`),
	}, sortInput(true))

	// assert local filesystem
	expected := fs.Expected(t, folderLayout...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// assert s3
	assert.Assert(t, ensureS3Object(s3client, bucket, "file1.txt", "this is the first test file"))
	assert.Assert(t, ensureS3Object(s3client, bucket, "readme.md", "this is a readme file"))
	assert.Assert(t, ensureS3Object(s3client, bucket, "file2.txt", "this is the second test file"))
}

func TestCopyMultipleFilesToS3(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	filesToContent := map[string]string{
		"testfile1.txt":          "this is a test file 1",
		"readme.md":              "this is a readme file",
		"filename-with-hypen.gz": "file has hypen in its name",
		"another_test_file.txt":  "yet another txt file. yatf.",
	}

	var files []fs.PathOp
	for filename, content := range filesToContent {
		op := fs.WithFile(filename, content)
		files = append(files, op)
	}

	workdir := fs.NewDir(t, "somedir", files...)
	defer workdir.Remove()

	cmd := s5cmd("cp", workdir.Path()+"/*", "s3://"+bucket+"/")
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: contains(` # Uploading another_test_file.txt...`),
		2: contains(` # Uploading filename-with-hypen.gz...`),
		3: contains(` # Uploading readme.md...`),
		4: contains(` # Uploading testfile1.txt...`),
	}, sortInput(true))

	// assert local filesystem
	expected := fs.Expected(t, files...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// assert s3
	for filename, content := range filesToContent {
		assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
	}
}

func TestCopySingleS3ObjectToS3(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	const (
		filename    = "testfile1.txt"
		dstfilename = "copy_" + filename
		content     = "this is a file content"
	)

	putFile(t, s3client, bucket, filename, content)

	src := fmt.Sprintf("s3://%v/%v", bucket, filename)
	dst := fmt.Sprintf("s3://%v/%v", bucket, dstfilename)

	cmd := s5cmd("cp", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`# Copying testfile1.txt...`),
	})

	// assert s3 source object
	assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))

	// assert s3 destination object
	assert.Assert(t, ensureS3Object(s3client, bucket, dstfilename, content))
}

func TestCopySingleS3ObjectIntoAnotherBucket(t *testing.T) {
	t.Parallel()

	srcbucket := s3BucketFromTestName(t)
	dstbucket := "copy-" + s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, srcbucket)
	createBucket(t, s3client, dstbucket)

	const (
		filename = "testfile1.txt"
		content  = "this is a file content"
	)

	putFile(t, s3client, srcbucket, filename, content)

	src := fmt.Sprintf("s3://%v/%v", srcbucket, filename)
	dst := fmt.Sprintf("s3://%v/%v", dstbucket, filename)

	cmd := s5cmd("cp", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`# Copying testfile1.txt...`),
	})

	// assert s3 source object
	assert.Assert(t, ensureS3Object(s3client, srcbucket, filename, content))

	// assert s3 destination object
	assert.Assert(t, ensureS3Object(s3client, dstbucket, filename, content))
}

func TestCopyMultipleS3ObjectsToS3(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	filesToContent := map[string]string{
		"testfile1.txt":          "this is a test file 1",
		"readme.md":              "this is a readme file",
		"filename-with-hypen.gz": "file has hypen in its name",
		"another_test_file.txt":  "yet another txt file. yatf.",
	}

	for filename, content := range filesToContent {
		putFile(t, s3client, bucket, filename, content)
	}

	src := fmt.Sprintf("s3://%v/*", bucket)
	dst := fmt.Sprintf("s3://%v/dst/", bucket)

	cmd := s5cmd("cp", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: contains(""),
		1: suffix(`# Copying another_test_file.txt...`),
		2: suffix(`# Copying filename-with-hypen.gz...`),
		3: suffix(`# Copying readme.md...`),
		4: suffix(`# Copying testfile1.txt...`),
	}, sortInput(true))

	// assert s3 source objects
	for filename, content := range filesToContent {
		assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
	}

	// assert s3 destination objects
	for filename, content := range filesToContent {
		assert.Assert(t, ensureS3Object(s3client, bucket, "dst/"+filename, content))
	}
}

func TestCopyMultipleS3ObjectsToS3_Issue70(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	filesToContent := map[string]string{
		"config/.local/folder1/file1.txt": "this is a test file 1",
		"config/.local/folder2/file2.txt": "this is a test file 2",
	}

	for filename, content := range filesToContent {
		putFile(t, s3client, bucket, filename, content)
	}

	src := fmt.Sprintf("s3://%v/config/.local/*", bucket)
	dst := fmt.Sprintf("s3://%v/.local/", bucket)

	cmd := s5cmd("cp", "-u", "-s", "--parents", src, dst)
	result := icmd.RunCmd(cmd)

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: contains(""),
		1: suffix(`# Copying file1.txt...`),
		2: suffix(`# Copying file2.txt...`),
	}, sortInput(true))

	// assert s3 source objects
	for filename, content := range filesToContent {
		assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
	}

	// assert s3 destination objects
	assert.Assert(t, ensureS3Object(s3client, bucket, ".local/folder1/file1.txt", "this is a test file 1"))
	assert.Assert(t, ensureS3Object(s3client, bucket, ".local/folder2/file2.txt", "this is a test file 2"))
}

func TestCopySingleLocalFileToLocal(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	const (
		filename    = "testfile1.txt"
		newFilename = "testfile1-copy.txt"
		content     = "this is a test file"
	)

	workdir := fs.NewDir(t, t.Name(), fs.WithFile(filename, content))
	defer workdir.Remove()

	cmd := s5cmd("cp", filename, newFilename)
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(" # Copying testfile1.txt..."),
	})

	// assert local filesystem
	expected := fs.Expected(
		t,
		fs.WithFile(filename, content),
		fs.WithFile(newFilename, content),
	)

	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

func TestCopyMultipleLocalFlatFilesToLocal(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	filesToContent := map[string]string{
		"testfile1.txt":          "this is a test file 1",
		"readme.md":              "this is a readme file",
		"filename-with-hypen.gz": "file has hypen in its name",
		"another_test_file.txt":  "yet another txt file. yatf.",
	}

	var files []fs.PathOp
	for filename, content := range filesToContent {
		op := fs.WithFile(filename, content)
		files = append(files, op)
	}

	workdir := fs.NewDir(t, t.Name(), files...)
	defer workdir.Remove()

	cmd := s5cmd("cp", "*.txt", "another-directory/")
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: equals(" # Copying another_test_file.txt..."),
		2: equals(" # Copying testfile1.txt..."),
	}, sortInput(true))

	// assert local filesystem
	expected := fs.Expected(
		t,
		fs.WithMode(0700),
		fs.WithFile("testfile1.txt", "this is a test file 1"),
		fs.WithFile("another_test_file.txt", "yet another txt file. yatf."),
		fs.WithFile("readme.md", "this is a readme file"),
		fs.WithFile("filename-with-hypen.gz", "file has hypen in its name"),
		fs.WithDir("another-directory",
			fs.WithMode(0755),
			fs.WithFile("testfile1.txt", "this is a test file 1"),
			fs.WithFile("another_test_file.txt", "yet another txt file. yatf."),
		),
	)

	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

func TestCopyMultipleLocalNestedFilesToLocal(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	// nested folder layout
	//
	// ├ a
	// │ └─readme.md
	// │ └─file1.txt
	// └─b
	//   └─c
	//     └─file2.txt
	//
	// after `s5cmd cp -R * dst`, expect:
	//
	// ├ dst
	// │ └─readme.md
	// │ └─file1.txt
	// │ └─file2.txt

	folderLayout := []fs.PathOp{
		fs.WithDir(
			"a",
			fs.WithFile("file1.txt", "this is the first test file"),
			fs.WithFile("readme.md", "this is a readme file"),
		),
		fs.WithDir(
			"b",
			fs.WithDir(
				"c",
				fs.WithFile("file2.txt", "this is the second test file"),
			),
		),
	}

	workdir := fs.NewDir(t, t.Name(), folderLayout...)
	defer workdir.Remove()

	cmd := s5cmd("cp", "-R", "*", "dst")
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: equals(" # Copying file1.txt..."),
		2: equals(" # Copying file2.txt..."),
		3: equals(" # Copying readme.md..."),
	}, sortInput(true))

	newLayout := append(folderLayout, fs.WithDir(
		"dst",
		fs.WithFile("file1.txt", "this is the first test file"),
		fs.WithFile("file2.txt", "this is the second test file"),
		fs.WithFile("readme.md", "this is a readme file"),
	),
	)

	expected := fs.Expected(t, newLayout...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

func TestCopyMultipleLocalNestedFilesToLocalPreserveLayout(t *testing.T) {
	t.Parallel()

	_, s5cmd, cleanup := setup(t)
	defer cleanup()

	// nested folder layout
	//
	// ├─a
	// │ ├─readme.md
	// │ └─file1.txt
	// └─b
	//   └─c
	//     └─file2.txt
	//
	// after `s5cmd cp -R --parents * dst`, expect:
	//
	// └dst
	//   ├─a
	//   │ ├─readme.md
	//   │ └─file1.txt
	//   └─b
	//     └─c
	//       └─file2.txt

	folderLayout := []fs.PathOp{
		fs.WithDir(
			"a",
			fs.WithFile("file1.txt", "this is the first test file"),
			fs.WithFile("readme.md", "this is a readme file"),
		),
		fs.WithDir(
			"b",
			fs.WithDir(
				"c",
				fs.WithFile("file2.txt", "this is the second test file"),
			),
		),
	}

	workdir := fs.NewDir(t, t.Name(), folderLayout...)
	defer workdir.Remove()

	cmd := s5cmd("cp", "-R", "--parents", "*", "dst")
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: equals(" # Copying file1.txt..."),
		2: equals(" # Copying file2.txt..."),
		3: equals(" # Copying readme.md..."),
	}, sortInput(true))

	newLayout := append(folderLayout, fs.WithDir("dst", folderLayout...))

	expected := fs.Expected(t, newLayout...)
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

func TestCopyS3ObjectToLocalWithTheSameFilename(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	const (
		filename        = "testfile1.txt"
		content         = "this is the content"
		expectedContent = content + "\n"
	)

	workdir := fs.NewDir(t, t.Name(), fs.WithFile(filename, content))
	defer workdir.Remove()

	createBucket(t, s3client, bucket)
	// upload a modified version of the file
	putFile(t, s3client, bucket, filename, expectedContent)

	cmd := s5cmd("cp", "s3://"+bucket+"/"+filename, ".")
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(" # Downloading %v...", filename),
	})

	expected := fs.Expected(t, fs.WithFile(filename, expectedContent))
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

func TestCopyS3ToLocalWithSameFilenameWithNoClobber(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	const (
		filename = "testfile1.txt"
		content  = "this is the content"
	)

	workdir := fs.NewDir(t, t.Name(), fs.WithFile(filename, content))
	defer workdir.Remove()

	createBucket(t, s3client, bucket)
	// upload a modified version of the file
	putFile(t, s3client, bucket, filename, content+"\n")

	cmd := s5cmd("cp", "-n", "s3://"+bucket+"/"+filename, ".")
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`WARNING "cp s3://%v/%v ./%v" (object already exists)`, bucket, filename, filename),
	})

	expected := fs.Expected(t, fs.WithFile(filename, content))
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

func TestCopyS3ToLocalWithSameFilenameOverrideIfSizeDiffers(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	const (
		filename        = "testfile1.txt"
		content         = "this is the content"
		expectedContent = content + "\n"
	)

	workdir := fs.NewDir(t, t.Name(), fs.WithFile(filename, content))
	defer workdir.Remove()

	createBucket(t, s3client, bucket)
	// upload a modified version of the file
	putFile(t, s3client, bucket, filename, expectedContent)

	cmd := s5cmd("cp", "-n", "-s", "s3://"+bucket+"/"+filename, ".")
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	// '-n' prevents overriding the file, but '-s' overrides '-n' if the file
	// size differs.
	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(` # Downloading %v...`, filename),
	})

	expected := fs.Expected(t, fs.WithFile(filename, expectedContent))
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

func TestCopyS3ToLocalWithSameFilenameOverrideIfSourceIsNewer(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	const (
		filename        = "testfile1.txt"
		content         = "this is the content"
		expectedContent = content + "\n"
	)

	now := time.Now().UTC()
	timestamp := fs.WithTimestamps(
		now.Add(-time.Minute), // access time
		now.Add(-time.Minute), // mod time
	)
	workdir := fs.NewDir(t, t.Name(), fs.WithFile(filename, content, timestamp))
	defer workdir.Remove()

	createBucket(t, s3client, bucket)
	// upload a modified version of the file. also uploaded file is newer than
	// the file on local fs.
	putFile(t, s3client, bucket, filename, expectedContent)

	cmd := s5cmd("cp", "-n", "-u", "s3://"+bucket+"/"+filename, ".")
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	// '-n' prevents overriding the file, but '-s' overrides '-n' if the file
	// size differs.
	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(` # Downloading %v...`, filename),
	})

	expected := fs.Expected(t, fs.WithFile(filename, expectedContent))
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

func TestCopyS3ToLocalWithSameFilenameDontOverrideIfS3ObjectIsOlder(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	const (
		filename = "testfile1.txt"
		content  = "this is the content"
	)

	createBucket(t, s3client, bucket)
	// upload a modified version of the file.
	putFile(t, s3client, bucket, filename, content+"\n")

	// file on the fs is newer than the file on s3. expect an 'dont override'
	// behaviour.
	now := time.Now().UTC()
	timestamp := fs.WithTimestamps(
		now.Add(time.Minute), // access time
		now.Add(time.Minute), // mod time
	)
	workdir := fs.NewDir(t, t.Name(), fs.WithFile(filename, content, timestamp))
	defer workdir.Remove()

	cmd := s5cmd("cp", "-n", "-u", "s3://"+bucket+"/"+filename, ".")
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	// '-n' prevents overriding the file, but '-s' overrides '-n' if the file
	// size differs.
	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`WARNING "cp s3://%v/%v ./%v" (object is newer or same age)`, bucket, filename, filename),
	})

	expected := fs.Expected(t, fs.WithFile(filename, content))
	assert.Assert(t, fs.Equal(workdir.Path(), expected))
}

func TestCopyS3ToLocal_Issue70(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	createBucket(t, s3client, bucket)

	filesToContent := map[string]string{
		"config/.local/folder1/file1.txt": "this is a test file 1",
		"config/.local/folder2/file2.txt": "this is a test file 2",
	}

	for filename, content := range filesToContent {
		putFile(t, s3client, bucket, filename, content)
	}

	workdir := fs.NewDir(t, t.Name())
	defer workdir.Remove()

	srcpath := fmt.Sprintf("s3://%v/config/.local/*", bucket)
	dstpath := filepath.Join(workdir.Path(), ".local")

	cmd := s5cmd("cp", "-u", "-s", "--parents", srcpath, dstpath)

	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: equals(""),
		1: suffix(`# Downloading file1.txt...`),
		2: suffix(`# Downloading file2.txt...`),
	}, sortInput(true))

	// assert local filesystem
	expectedFiles := []fs.PathOp{
		fs.WithDir(
			".local",
			fs.WithMode(0755),
			fs.WithDir("folder1", fs.WithMode(0755), fs.WithFile("file1.txt", "this is a test file 1")),
			fs.WithDir("folder2", fs.WithMode(0755), fs.WithFile("file2.txt", "this is a test file 2")),
		),
	}

	expectedResult := fs.Expected(t, expectedFiles...)
	assert.Assert(t, fs.Equal(workdir.Path(), expectedResult))

	// assert s3 objects
	for filename, content := range filesToContent {
		assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
	}
}

func TestCopyLocalFileToS3WithTheSameFilename(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	const (
		filename   = "testfile1.txt"
		content    = "this is the content"
		newContent = content + "\n"
	)

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, filename, content)

	// the file to be uploaded is modified
	workdir := fs.NewDir(t, t.Name(), fs.WithFile(filename, newContent))
	defer workdir.Remove()

	cmd := s5cmd("cp", filename, "s3://"+bucket)
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(` # Uploading %v...`, filename),
	})

	// assert local filesystem
	expected := fs.Expected(t, fs.WithFile(filename, newContent))
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// expect s3 object to be updated with new content
	assert.Assert(t, ensureS3Object(s3client, bucket, filename, newContent))
}

func TestCopyLocalFileToS3WithSameFilenameWithNoClobber(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	const (
		filename   = "testfile1.txt"
		content    = "this is the content"
		newContent = content + "\n"
	)

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, filename, content)

	// the file to be uploaded is modified
	workdir := fs.NewDir(t, t.Name(), fs.WithFile(filename, newContent))
	defer workdir.Remove()

	cmd := s5cmd("cp", "-n", filename, "s3://"+bucket)
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`WARNING "cp %v s3://%v/%v" (object already exists)`, filename, bucket, filename),
	})

	// assert local filesystem
	expected := fs.Expected(t, fs.WithFile(filename, newContent))
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// expect s3 object is not overriden
	assert.Assert(t, ensureS3Object(s3client, bucket, filename, content))
}

func TestCopyLocalFileToS3WithNoClobber(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	const (
		filename   = "testfile1.txt"
		content    = "this is the content"
		newContent = content + "\n"
	)

	createBucket(t, s3client, bucket)

	// the file to be uploaded is modified
	workdir := fs.NewDir(t, t.Name(), fs.WithFile(filename, newContent))
	defer workdir.Remove()

	cmd := s5cmd("cp", "-n", filename, "s3://"+bucket)
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: contains(` # Uploading %v...`, filename),
	})

	// assert local filesystem
	expected := fs.Expected(t, fs.WithFile(filename, newContent))
	assert.Assert(t, fs.Equal(workdir.Path(), expected))

	// expect s3 object is not overriden
	assert.Assert(t, ensureS3Object(s3client, bucket, filename, newContent))
}

func TestCopyLocalFileToS3WithSameFilenameOverrideIfSizeDiffers(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	const (
		filename        = "testfile1.txt"
		content         = "this is the content"
		expectedContent = content + "\n"
	)

	workdir := fs.NewDir(t, t.Name(), fs.WithFile(filename, expectedContent))
	defer workdir.Remove()

	createBucket(t, s3client, bucket)
	// upload a modified version of the file
	putFile(t, s3client, bucket, filename, content)

	cmd := s5cmd("cp", "-n", "-s", filename, "s3://"+bucket)
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	// '-n' prevents overriding the file, but '-s' overrides '-n' if the file
	// size differs.
	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(` # Uploading %v...`, filename),
	})

	assert.NilError(t, ensureS3Object(s3client, bucket, filename, expectedContent))
}

func TestCopyLocalFileToS3WithSameFilenameOverrideIfSourceIsNewer(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	const (
		filename        = "testfile1.txt"
		content         = "this is the content"
		expectedContent = content + "\n"
	)

	createBucket(t, s3client, bucket)
	// upload a modified version of the file. also uploaded file is newer than
	// the file on local fs.
	putFile(t, s3client, bucket, filename, content)

	now := time.Now().UTC()
	timestamp := fs.WithTimestamps(
		now.Add(time.Minute), // access time
		now.Add(time.Minute), // mod time
	)
	workdir := fs.NewDir(t, t.Name(), fs.WithFile(filename, expectedContent, timestamp))
	defer workdir.Remove()

	cmd := s5cmd("cp", "-n", "-u", filename, "s3://"+bucket)
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	// '-n' prevents overriding the file, but '-u' overrides '-n' if the file
	// modtime differs.
	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(` # Uploading %v...`, filename),
	})

	assert.NilError(t, ensureS3Object(s3client, bucket, filename, expectedContent))
}

func TestCopyLocalFileToS3WithSameFilenameDontOverrideIfS3ObjectIsOlder(t *testing.T) {
	t.Parallel()

	bucket := s3BucketFromTestName(t)

	s3client, s5cmd, cleanup := setup(t)
	defer cleanup()

	const (
		filename        = "testfile1.txt"
		content         = "this is the content"
		expectedContent = content + "\n"
	)

	createBucket(t, s3client, bucket)
	// upload a modified version of the file. also uploaded file is newer than
	// the file on local fs.
	putFile(t, s3client, bucket, filename, content)

	now := time.Now().UTC()
	timestamp := fs.WithTimestamps(
		now.Add(-time.Minute), // access time
		now.Add(-time.Minute), // mod time
	)
	workdir := fs.NewDir(t, t.Name(), fs.WithFile(filename, expectedContent, timestamp))
	defer workdir.Remove()

	cmd := s5cmd("cp", "-n", "-u", filename, "s3://"+bucket)
	result := icmd.RunCmd(cmd, withWorkingDir(workdir))

	// '-n' prevents overriding the file, but '-u' overrides '-n' if the file
	// modtime differs.
	result.Assert(t, icmd.Success)

	assertLines(t, result.Stdout(), map[int]compareFunc{
		0: suffix(`WARNING "cp %v s3://%v/%v" (object is newer or same age)`, filename, bucket, filename),
	})

	assert.NilError(t, ensureS3Object(s3client, bucket, filename, content))
}
