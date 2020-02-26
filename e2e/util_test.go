// Package e2e contains tests that run against a real s5cmd binary,
// compiled on the fly at the start of the test run.
package e2e

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/go-cmp/cmp"
	"github.com/iancoleman/strcase"
	"gotest.tools/v3/assert"
	"gotest.tools/v3/fs"
	"gotest.tools/v3/icmd"
)

var (
	defaultAccessKeyID     = "s5cmd-test-access-key-id"
	defaultSecretAccessKey = "s5cmd-test-secret-access-key"
)

// dateRe is the same <YYYY/MM/dd HH:mm:ss> string use throughout most command
// outputs.
var dateRe = `(\d{4}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2})`

var (
	flagTestLogLevel = flag.String("test.log.level", "err", "Test log level: {debug|warn|err}")
	s5cmdPath        string
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type setupOpts struct {
	s3backend string
}

type option func(*setupOpts)

func withS3Backend(backend string) option {
	return func(opts *setupOpts) {
		opts.s3backend = backend
	}
}

func setup(t *testing.T, options ...option) (*s3.S3, func(...string) icmd.Cmd, func()) {
	t.Helper()

	opts := &setupOpts{
		s3backend: "bolt",
	}

	for _, option := range options {
		option(opts)
	}

	testdir := fs.NewDir(t, t.Name(), fs.WithDir("workdir", fs.WithMode(0700)))
	workdir := testdir.Join("workdir")

	var (
		s3LogLevel  = *flagTestLogLevel
		awsLogLevel = aws.LogOff
	)

	switch *flagTestLogLevel {
	case "debug":
		s3LogLevel = "info"
		// aws has no level other than 'debug'
		awsLogLevel = aws.LogDebug
	}

	endpoint, dbcleanup := s3ServerEndpoint(t, testdir, s3LogLevel, opts.s3backend)

	s3Config := aws.NewConfig().
		WithEndpoint(endpoint).
		WithRegion("us-east-1").
		WithCredentials(credentials.NewStaticCredentials(defaultAccessKeyID, defaultSecretAccessKey, "")).
		WithDisableSSL(true).
		WithS3ForcePathStyle(true).
		WithCredentialsChainVerboseErrors(true).
		WithLogLevel(awsLogLevel)

	sess, err := session.NewSession(s3Config)
	assert.NilError(t, err)

	s5cmd := func(args ...string) icmd.Cmd {
		endpoint := []string{"-endpoint-url", endpoint}
		args = append(endpoint, args...)

		cmd := icmd.Command(s5cmdPath, args...)
		env := os.Environ()
		env = append(
			env,
			[]string{
				fmt.Sprintf("AWS_ACCESS_KEY_ID=%v", defaultAccessKeyID),
				fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%v", defaultSecretAccessKey),
			}...,
		)
		cmd.Env = env
		cmd.Dir = workdir
		return cmd
	}

	cleanup := func() {
		testdir.Remove()
		dbcleanup()
	}

	return s3.New(sess), s5cmd, cleanup
}

func goBuildS5cmd() func() {
	tmpdir, err := ioutil.TempDir("", "")
	if err != nil {
		panic(err)
	}

	s5cmdPath = filepath.Join(tmpdir, "s5cmd")

	workdir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	// 'go build' will change the working directory to the path where tests
	// reside. workdir should be the project root.
	workdir = filepath.Dir(workdir)

	cmd := exec.Command(
		"go", "build",
		"-mod=vendor",
		"-o", s5cmdPath,
	)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	cmd.Dir = workdir

	if err := cmd.Run(); err != nil {
		// The go compiler will have already produced some error messages
		// on stderr by the time we get here.
		panic(fmt.Sprintf("failed to build executable: %s", err))
	}

	if err := os.Chmod(s5cmdPath, 0755); err != nil {
		panic(err)
	}

	return func() {
		os.RemoveAll(tmpdir)
	}
}

func createBucket(t *testing.T, client *s3.S3, bucket string) {
	t.Helper()

	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}

	_, err := client.CreateBucket(input)
	if err != nil {
		t.Fatal(err)
	}
}

var errS3NoSuchKey = fmt.Errorf("s3: no such key")

func ensureS3Object(client *s3.S3, bucket string, key string, expectedContent string) error {
	output, err := client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	awsErr, ok := err.(awserr.Error)
	if ok {
		switch awsErr.Code() {
		case s3.ErrCodeNoSuchKey:
			return fmt.Errorf("%v: %w", key, errS3NoSuchKey)
		}
	}
	if err != nil {
		return err
	}

	var body bytes.Buffer
	if _, err := io.Copy(&body, output.Body); err != nil {
		return err
	}
	defer output.Body.Close()

	if diff := cmp.Diff(expectedContent, body.String()); diff != "" {
		return fmt.Errorf("s3 %v/%v: (-want +got):\n%v", bucket, key, diff)
	}

	return nil
}

func putFile(t *testing.T, client *s3.S3, bucket string, filename string, content string) {
	t.Helper()

	_, err := client.PutObject(&s3.PutObjectInput{
		Body:   strings.NewReader(content),
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func replaceMatchWithSpace(input string, match ...string) string {
	for _, m := range match {
		if m == "" {
			continue
		}
		re := regexp.MustCompile(m)
		input = re.ReplaceAllString(input, " ")
	}

	return input
}

func s3BucketFromTestName(t *testing.T) string {
	t.Helper()
	bucket := strcase.ToKebab(t.Name())

	if len(bucket) > 63 {
		bucket = fmt.Sprintf("%v-%v", bucket[:55], randomString(7))
	}

	return bucket
}

func randomString(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return string(b)
}

func withWorkingDir(dir *fs.Dir) func(*icmd.Cmd) {
	return func(cmd *icmd.Cmd) {
		cmd.Dir = dir.Path()
	}
}

type compareFunc func(string) error

type assertOpts struct {
	strict      bool
	sort        bool
	trimRegexes []*regexp.Regexp
}

type assertOp func(*assertOpts)

func sortInput(v bool) func(*assertOpts) {
	return func(opts *assertOpts) {
		opts.sort = v
	}
}

func strictLineCheck(v bool) func(*assertOpts) {
	return func(opts *assertOpts) {
		opts.strict = v
	}
}

func trimMatch(match string) func(*assertOpts) {
	re := regexp.MustCompile(match)
	return func(opts *assertOpts) {
		opts.trimRegexes = append(opts.trimRegexes, re)
	}
}

func assertError(t *testing.T, err error, expected interface{}) {
	t.Helper()
	// 'assert' package doesn't support Go1.13+ error unwrapping. Do it
	// manually.
	assert.ErrorType(t, errors.Unwrap(err), expected)
}

func assertLines(t *testing.T, actual string, expectedlines map[int]compareFunc, fns ...assertOp) {
	t.Helper()

	// default assertion options
	opts := assertOpts{
		strict:      true,
		sort:        false,
		trimRegexes: nil,
	}

	for _, fn := range fns {
		fn(&opts)
	}

	for _, re := range opts.trimRegexes {
		actual = re.ReplaceAllString(actual, "")
	}

	lines := strings.Split(actual, "\n")
	if opts.sort {
		sort.Strings(lines)
	}

	if len(expectedlines) > len(lines) {
		t.Errorf(
			"expected lines (count: %v) should be <= actual lines (count: %v)",
			len(expectedlines),
			len(lines),
		)
	}

	for i, line := range lines {
		// trim consecutive spaces
		line = replaceMatchWithSpace(line, `\s+`)

		// trim last excessive new line. this one does not affect the output
		// testing since it's the last newline character to make the shell
		// prompt look nice.
		if i == len(lines)-1 && line == "" {
			continue
		}

		cmp, ok := expectedlines[i]
		if !ok {
			if opts.strict {
				t.Errorf("expected a comparison function for line %q (lineno: %v)", line, i)
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
		if re.MatchString(actual) {
			return nil
		}
		return fmt.Errorf("match: given %q regex doesn't match with %q", expected, actual)
	}
}

func equals(format string, args ...interface{}) compareFunc {
	expected := fmt.Sprintf(format, args...)
	return func(actual string) error {
		if expected == actual {
			return nil
		}

		diff := cmp.Diff(expected, actual)
		return fmt.Errorf("equals: (-want +got):\n%v", diff)
	}
}

func prefix(format string, args ...interface{}) compareFunc {
	expected := fmt.Sprintf(format, args...)
	return func(actual string) error {
		if strings.HasPrefix(actual, expected) {
			return nil
		}

		diff := cmp.Diff(expected, actual)
		return fmt.Errorf("prefix: (-want +got):\n%v", diff)
	}
}

func suffix(format string, args ...interface{}) compareFunc {
	expected := fmt.Sprintf(format, args...)
	return func(actual string) error {
		if strings.HasSuffix(actual, expected) {
			return nil
		}

		diff := cmp.Diff(expected, actual)
		return fmt.Errorf("suffix: (-want +got):\n%v", diff)
	}
}

func contains(format string, args ...interface{}) compareFunc {
	expected := fmt.Sprintf(format, args...)
	return func(actual string) error {
		if strings.Contains(actual, expected) {
			return nil
		}

		diff := cmp.Diff(expected, actual)
		return fmt.Errorf("contains: (-want +got):\n%v", diff)
	}
}
