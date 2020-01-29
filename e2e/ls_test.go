package main

import (
	"fmt"
	"io/ioutil"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/go-cmp/cmp"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3bolt"
	"gotest.tools/icmd"
)

var (
	defaultAccessKeyID     = "s5cmd-test-access-key-id"
	defaultSecretAccessKey = "s5cmd-test-secret-access-key"
)

func TestListBuckets(t *testing.T) {
	const dbname = "test-list-buckets"

	s3client, s5cmd, cleanup := setup(t, dbname)
	defer cleanup()

	// alphabetically unordered list of buckets
	createBucket(t, s3client, "test-list-buckets-1")
	createBucket(t, s3client, "test-list-buckets-2")
	createBucket(t, s3client, "test-list-buckets-4")
	createBucket(t, s3client, "test-list-buckets-3")

	cmd := s5cmd("ls")

	result := icmd.RunCmd(
		cmd,
		icmd.WithEnv(
			fmt.Sprintf("AWS_ACCESS_KEY_ID=%v", defaultAccessKeyID),
			fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%v", defaultSecretAccessKey),
		),
	)

	result.Assert(t, icmd.Success)
	result.Assert(t, icmd.Expected{Err: `+OK "ls"`})

	// expect and ordered list
	assert(t, result.Stdout(), map[int]compareFunc{
		0: suffix("s3://test-list-buckets-1"),
		1: suffix("s3://test-list-buckets-2"),
		2: suffix("s3://test-list-buckets-3"),
		3: suffix("s3://test-list-buckets-4"),
		4: equals(""),
	}, true)
}

func TestListSingleS3Object(t *testing.T) {
	const (
		bucket = "test-list-single-s3-object"
		dbname = bucket
	)

	s3client, s5cmd, cleanup := setup(t, dbname)
	defer cleanup()

	createBucket(t, s3client, bucket)

	// create 2 files, expect 1.
	putFile(t, s3client, bucket, "testfile1.txt", "this is a file content")
	putFile(t, s3client, bucket, "testfile2.txt", "this is also a file content")

	cmd := s5cmd("ls", "s3://"+bucket+"/testfile1.txt")

	result := icmd.RunCmd(
		cmd,
		icmd.WithEnv(
			fmt.Sprintf("AWS_ACCESS_KEY_ID=%v", defaultAccessKeyID),
			fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%v", defaultSecretAccessKey),
		),
	)

	result.Assert(t, icmd.Success)

	assert(t, result.Stderr(), map[int]compareFunc{
		0: suffix(`+OK "ls s3://test-list-single-s3-object/testfile1.txt" (1)`),
	}, false)

	assert(t, result.Stdout(), map[int]compareFunc{
		// 0: suffix("317 testfile1.txt"),
		0: match(`\s+(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*testfile1.txt`),
		1: equals(""),
	}, true)
}

func TestListSingleWildcardS3Object(t *testing.T) {
	const (
		bucket = "test-list-wildcard-s3-object"
		dbname = bucket
	)

	s3client, s5cmd, cleanup := setup(t, dbname)
	defer cleanup()

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "testfile1.txt", "this is a file content")
	putFile(t, s3client, bucket, "testfile2.txt", "this is also a file content")
	putFile(t, s3client, bucket, "testfile3.txt", "this is also a file content somehow")

	cmd := s5cmd("ls", "s3://"+bucket+"/*.txt")

	result := icmd.RunCmd(
		cmd,
		icmd.WithEnv(
			fmt.Sprintf("AWS_ACCESS_KEY_ID=%v", defaultAccessKeyID),
			fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%v", defaultSecretAccessKey),
		),
	)

	result.Assert(t, icmd.Success)

	assert(t, result.Stderr(), map[int]compareFunc{
		0: suffix(`+OK "ls s3://test-list-wildcard-s3-object/*.txt" (3)`),
	}, false)

	assert(t, result.Stdout(), map[int]compareFunc{
		0: suffix("317 testfile1.txt"),
		1: suffix("322 testfile2.txt"),
		2: suffix("330 testfile3.txt"),
		3: equals(""),
	}, true)
}

func TestListMultipleWildcardS3Object(t *testing.T) {
	const (
		bucket = "test-list-wildcard-s3-object"
		dbname = bucket
	)

	s3client, s5cmd, cleanup := setup(t, dbname)
	defer cleanup()

	createBucket(t, s3client, bucket)
	putFile(t, s3client, bucket, "/a/testfile1.txt", "content")
	putFile(t, s3client, bucket, "/a/testfile2.txt", "content")
	putFile(t, s3client, bucket, "/b/testfile3.txt", "content")
	putFile(t, s3client, bucket, "/b/testfile4.txt", "content")
	putFile(t, s3client, bucket, "/c/testfile5.gz", "content")
	putFile(t, s3client, bucket, "/c/testfile6.txt.gz", "content")
	putFile(t, s3client, bucket, "/d/foo/bar/file7.txt", "content")
	putFile(t, s3client, bucket, "/d/foo/bar/testfile8.txt", "content")
	putFile(t, s3client, bucket, "/e/txt/testfile9.txt.gz", "content")
	putFile(t, s3client, bucket, "/f/txt/testfile10.txt", "content")

	const pattern = "/*/testfile*.txt"
	cmd := s5cmd("ls", "s3://"+bucket+pattern)

	result := icmd.RunCmd(
		cmd,
		icmd.WithEnv(
			fmt.Sprintf("AWS_ACCESS_KEY_ID=%v", defaultAccessKeyID),
			fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%v", defaultSecretAccessKey),
		),
	)

	result.Assert(t, icmd.Success)

	assert(t, result.Stderr(), map[int]compareFunc{
		0: suffix(`+OK "ls s3://test-list-wildcard-s3-object/*/testfile*.txt" (6)`),
	}, false)

	assert(t, result.Stdout(), map[int]compareFunc{
		0: suffix("304 a/testfile1.txt"),
		1: suffix("304 a/testfile2.txt"),
		2: suffix("304 b/testfile3.txt"),
		3: suffix("304 b/testfile4.txt"),
		4: suffix("312 d/foo/bar/testfile8.txt"),
		5: suffix("309 f/txt/testfile10.txt"),
		6: equals(""),
	}, true)
}

func setup(t *testing.T, dbname string) (*s3.S3, func(...string) icmd.Cmd, func()) {
	dbdir := filepath.Join(os.TempDir(), "s5cmd-test")
	err := os.MkdirAll(dbdir, 0755)
	if err != nil {
		t.Fatal(err)
	}

	// we use boltdb as the s3 backend because listing buckets in in-memory
	// backend is not deterministic.
	dbpath := filepath.Join(dbdir, dbname+".boltdb")
	backend, err := s3bolt.NewFile(dbpath)
	if err != nil {
		t.Fatal(err)
	}

	faker := gofakes3.New(backend)
	s3srv := httptest.NewServer(faker.Server())

	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials("YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", ""),
		Endpoint:         aws.String(s3srv.URL),
		Region:           aws.String("us-east-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}

	sess := session.New(s3Config)

	s5cmd := func(args ...string) icmd.Cmd {
		endpoint := []string{"-endpoint-url", s3srv.URL}
		args = append(endpoint, args...)
		return icmd.Command("s5cmd", args...)
	}

	cleanup := func() {
		os.Remove(dbpath)
		s3srv.Close()
	}

	return s3.New(sess), s5cmd, cleanup
}

func createBucket(t *testing.T, client *s3.S3, bucket string) {
	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}

	_, err := client.CreateBucket(input)
	if err != nil {
		t.Fatal(err)
	}
}

func putFile(t *testing.T, client *s3.S3, bucket string, filename string, content string) {
	// Upload a new object "testobject" with the string "Hello World!" to our "newbucket".
	_, err := client.PutObject(&s3.PutObjectInput{
		Body:   strings.NewReader(content),
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func createFiles(t *testing.T, filenames []string, contents [][]byte) {
	if len(filenames) != len(contents) {
		t.Fatal("createFiles: filename and content should be given as pair")
	}

	dir, err := ioutil.TempDir("", "s5cmd-test")
	if err != nil {
		t.Fatal(err)
	}

	for i, filename := range filenames {
		content := contents[i]
		fpath := filepath.Join(dir, filename)
		err := ioutil.WriteFile(fpath, content, 0644)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func replaceWithSpace(input string, match ...string) string {
	match = append(match, `\s+`)
	for _, m := range match {
		if m == "" {
			continue
		}
		re := regexp.MustCompile(m)
		input = re.ReplaceAllString(input, " ")
	}

	return input
}

type compareFunc func(string) error

func assert(t *testing.T, actual string, expectedlines map[int]compareFunc, strict bool) {
	t.Helper()

	lines := strings.Split(actual, "\n")

	for i, line := range lines {
		line = replaceWithSpace(line)
		cmp, ok := expectedlines[i]
		if !ok {
			if strict {
				t.Fatalf("expected a comparison function for line %q (lineno: %v)", line, i)
			}
			continue
		}

		if err := cmp(line); err != nil {
			t.Errorf("line %v: %v", i, err)
		}
	}

	if t.Failed() {
		t.Log(actual)
	}
}

func match(expected string) compareFunc {
	re := regexp.MustCompile(expected)
	return func(actual string) error {
		fmt.Printf("%q\n", re.FindStringSubmatch(actual))
		if re.MatchString(actual) {
			return nil
		}
		return fmt.Errorf("match: given %q regex doesn't match with %q", expected, actual)
	}
}

func equals(expected string) compareFunc {
	return func(actual string) error {
		if expected == actual {
			return nil
		}

		diff := cmp.Diff(expected, actual)
		return fmt.Errorf("equals: (-want +got):\n%v", diff)
	}
}

func prefix(expected string) compareFunc {
	return func(actual string) error {
		if strings.HasPrefix(actual, expected) {
			return nil
		}

		diff := cmp.Diff(expected, actual)
		return fmt.Errorf("prefix: (-want +got):\n%v", diff)
	}
}

func suffix(expected string) compareFunc {
	return func(actual string) error {
		if strings.HasSuffix(actual, expected) {
			return nil
		}

		diff := cmp.Diff(expected, actual)
		return fmt.Errorf("suffix: (-want +got):\n%v", diff)
	}
}

func contains(expected string) compareFunc {
	return func(actual string) error {
		if strings.Contains(actual, expected) {
			return nil
		}

		diff := cmp.Diff(expected, actual)
		return fmt.Errorf("contains: (-want +got):\n%v", diff)
	}
}
