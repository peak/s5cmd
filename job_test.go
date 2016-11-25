package s5cmd

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
)

func newJob(sourceDesc, command string, operation Operation, args []*JobArgument, opts OptionType) Job {
	return Job{
		sourceDesc: sourceDesc,
		command:    command,
		operation:  operation,
		args:       args,
		opts:       opts,
	}
}

var (
	result        error
	stats         = Stats{}
	idlingCounter int32
	subJobQueue   chan *Job = make(chan *Job)
	wp                      = WorkerParams{
		nil,
		nil,
		nil,
		context.TODO(),
		nil,
		&stats,
		&subJobQueue,
		&idlingCounter,
	}

	// These Jobs are used for benchmarks and also as skeletons for tests
	localCopyJob = newJob("!cp-test", "!cp", OP_LOCAL_COPY,
		[]*JobArgument{
			{"test-src", nil},
			{"test-dst", nil},
		}, OPT_NONE)
	localMoveJob = newJob("!mv-test", "!mv", OP_LOCAL_COPY,
		[]*JobArgument{
			{"test-src", nil},
			{"test-dst", nil},
		}, OPT_DELETE_SOURCE)
	localDeleteJob = newJob("!rm-test", "!rm", OP_LOCAL_DELETE,
		[]*JobArgument{
			{"test-src", nil},
		}, OPT_NONE)
)

func benchmarkJobRun(b *testing.B, j *Job) {
	var err error

	for n := 0; n < b.N; n++ {
		createFile("test-src", "")
		err = j.Run(&wp)
	}

	deleteFile("test-dst")
	result = err
}

func BenchmarkJobRunLocalCopy(b *testing.B) {
	benchmarkJobRun(b, &localCopyJob)
}

func BenchmarkJobRunLocalMove(b *testing.B) {
	benchmarkJobRun(b, &localMoveJob)
}

func BenchmarkJobRunLocalDelete(b *testing.B) {
	benchmarkJobRun(b, &localDeleteJob)
}

func createFile(filename, contents string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	f.WriteString(contents)
	return nil
}

func readFile(filename string) (string, error) {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func deleteFile(filename string) {
	os.Remove(filename)
}

func tempFile(prefix string) (string, error) {
	f, err := ioutil.TempFile("", prefix)
	if err != nil {
		return "", err
	}
	filename := f.Name()
	f.Close()
	deleteFile(filename)

	return filename, nil
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

func TestJobRunLocalDelete(t *testing.T) {
	// setup
	fn, err := tempFile("localdelete")
	if err != nil {
		t.Fatal(err)
	}
	err = createFile(fn, "contents")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Created temp file: %s", fn)

	old_args := localDeleteJob.args

	localDeleteJob.args = []*JobArgument{
		{fn, nil},
	}

	// execute
	err = localDeleteJob.Run(&wp)
	if err != nil {
		t.Error(err)
	}

	// verify
	if fileExists(fn) {
		t.Error("File should not exist after delete")
	}

	// teardown
	deleteFile(fn)

	localDeleteJob.args = old_args
}

func testLocalCopyOrMove(t *testing.T, is_move bool) {
	// setup
	src, err := tempFile("src")
	if err != nil {
		t.Fatal(err)
	}
	file_contents := "contents"
	err = createFile(src, file_contents)
	if err != nil {
		t.Fatal(err)
	}

	var job *Job
	if is_move {
		job = &localMoveJob
	} else {
		job = &localCopyJob
	}

	old_args := job.args

	dst := ""

	for { // teardown after this

		dst, err = tempFile("dst")
		if err != nil {
			t.Error(err)
			break
		}

		t.Logf("Created temp files: src=%s dst=%s", src, dst)

		job.args = []*JobArgument{
			{src, nil},
			{dst, nil},
		}

		// execute
		err = job.Run(&wp)
		if err != nil {
			t.Error(err)
			break
		}

		// verify
		if is_move {
			if fileExists(src) {
				t.Error("src should not exist after move")
				break
			}
		}

		new_contents, err := readFile(dst)
		if err != nil {
			t.Error(err)
			break
		}

		if new_contents != file_contents {
			t.Error("File contents do not match")
		}

		break // always break
	}

	// teardown
	deleteFile(src)
	if dst != "" {
		deleteFile(dst)
	}

	job.args = old_args
}

func TestJobRunLocalCopy(t *testing.T) {
	testLocalCopyOrMove(t, false)
}

func TestJobRunLocalMove(t *testing.T) {
	testLocalCopyOrMove(t, true)
}
