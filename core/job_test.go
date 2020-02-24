package core

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/stats"
	"github.com/peak/s5cmd/storage"
)

func newJob(sourceDesc, command string, operation op.Operation, args []*JobArgument, opts opt.OptionList) Job {
	return Job{
		sourceDesc: sourceDesc,
		command:    command,
		operation:  operation,
		args:       args,
		opts:       opts,
	}
}

func newURL(s string) *objurl.ObjectURL {
	url, _ := objurl.New(s)
	return url
}

var (
	result        error
	st            = stats.Stats{}
	idlingCounter int32
	subJobQueue   = make(chan *Job)
	wp            = WorkerParams{
		ctx:           context.TODO(),
		poolParams:    nil,
		st:            &st,
		subJobQueue:   &subJobQueue,
		idlingCounter: &idlingCounter,
		newClient: func(url *objurl.ObjectURL) (storage.Storage, error) {
			if url.IsRemote() {
				panic("remote url is not expected")
			}

			return storage.NewFilesystem(), nil
		},
	}

	// These Jobs are used for benchmarks and also as skeletons for tests
	localCopyJob = newJob("!cp-test", "!cp", op.LocalCopy,
		[]*JobArgument{
			{url: newURL("test-src")},
			{url: newURL("test-dst")},
		},
		opt.OptionList{},
	)

	localMoveJob = newJob("!mv-test", "!mv", op.LocalCopy,
		[]*JobArgument{
			{url: newURL("test-src")},
			{url: newURL("test-dst")},
		},
		opt.OptionList{opt.DeleteSource},
	)

	localDeleteJob = newJob("!rm-test", "!rm", op.LocalDelete,
		[]*JobArgument{
			{url: newURL("test-src")},
		},
		opt.OptionList{},
	)
)

func benchmarkJobRun(b *testing.B, j *Job) {

	for n := 0; n < b.N; n++ {
		createFile("test-src", "")
		_ = j.run(&wp)
	}

	deleteFile("test-dst")
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
	filename, err := tempFile("localdelete")
	if err != nil {
		t.Fatal(err)
	}

	err = createFile(filename, "contents")
	if err != nil {
		t.Fatal(err)
	}
	defer deleteFile(filename)

	oldArgs := localDeleteJob.args

	localDeleteJob.args = []*JobArgument{
		{url: newURL(filename)},
	}

	// execute
	resp := localDeleteJob.run(&wp)
	if resp.err != nil {
		t.Error(resp.err)
	}

	// verify
	if fileExists(filename) {
		t.Error("File should not exist after delete")
	}

	localDeleteJob.args = oldArgs
}

func testLocalCopyOrMove(t *testing.T, isMove bool) {
	// setup
	src, err := tempFile("src")
	if err != nil {
		t.Fatal(err)
	}
	fileContents := "contents"
	err = createFile(src, fileContents)
	if err != nil {
		t.Fatal(err)
	}

	var job *Job
	if isMove {
		job = &localMoveJob
	} else {
		job = &localCopyJob
	}

	oldArgs := job.args
	dst := ""

	// teardown
	defer func() {
		deleteFile(src)
		if dst != "" {
			deleteFile(dst)
		}

		job.args = oldArgs
	}()

	dst, err = tempFile("dst")
	if err != nil {
		t.Error(err)
		return
	}

	job.args = []*JobArgument{
		{url: newURL(src)},
		{url: newURL(dst)},
	}

	// execute
	resp := job.run(&wp)
	if resp.err != nil {
		t.Error(resp.err)
		return
	}

	// verify
	if isMove {
		if fileExists(src) {
			t.Error("src should not exist after move")
			return
		}
	}

	newContents, err := readFile(dst)
	if err != nil {
		t.Error(err)
		return
	}

	if newContents != fileContents {
		t.Error("File contents do not match")
	}
}

func TestJobRunLocalCopy(t *testing.T) {
	testLocalCopyOrMove(t, false)
}

func TestJobRunLocalMove(t *testing.T) {
	testLocalCopyOrMove(t, true)
}
