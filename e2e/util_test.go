// Package e2e contains tests that run against a real s5cmd binary,
// compiled on the fly at the start of the test run.
package e2e

import (
	"bytes"
	jsonpkg "encoding/json"
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
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/peak/s5cmd/storage"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
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

	endpoint, workdir, cleanup := server(t, opts.s3backend)

	client := s3client(t, storage.Options{
		Endpoint:    endpoint,
		NoVerifySSL: true,
	})

	return client, s5cmd(workdir, endpoint), cleanup
}

func server(t *testing.T, s3backend string) (string, string, func()) {
	t.Helper()

	// testdir := fs.NewDir() tries to create a new directory which
	// has a prefix = [test function name][operation name]
	// e.g., prefix' = "TestCopySingleS3ObjectToLocal/cp_s3://bucket/object_file"
	// but on windows, directories cannot contain a colon
	// so we replace them with hyphen
	prefix := t.Name()
	if runtime.GOOS == "windows" {
		prefix = strings.ReplaceAll(prefix, ":", "-")
	}

	testdir := fs.NewDir(t, prefix, fs.WithDir("workdir", fs.WithMode(0700)))
	workdir := testdir.Join("workdir")

	s3LogLevel := *flagTestLogLevel

	if *flagTestLogLevel == "debug" {
		s3LogLevel = "info" // aws has no level other than 'debug'
	}

	endpoint, dbcleanup := s3ServerEndpoint(t, testdir, s3LogLevel, s3backend)

	cleanup := func() {
		testdir.Remove()
		dbcleanup()
	}

	return endpoint, workdir, cleanup
}

func s3client(t *testing.T, options storage.Options) *s3.S3 {
	t.Helper()

	awsLogLevel := aws.LogOff
	if *flagTestLogLevel == "debug" {
		awsLogLevel = aws.LogDebug
	}

	s3Config := aws.NewConfig().
		WithEndpoint(options.Endpoint).
		WithRegion(endpoints.UsEast1RegionID).
		WithCredentials(credentials.NewStaticCredentials(defaultAccessKeyID, defaultSecretAccessKey, "")).
		WithDisableSSL(options.NoVerifySSL).
		WithS3ForcePathStyle(true).
		WithCredentialsChainVerboseErrors(true).
		WithLogLevel(awsLogLevel)

	sess, err := session.NewSession(s3Config)
	assert.NilError(t, err)

	return s3.New(sess)
}

func s5cmd(workdir, endpoint string) func(args ...string) icmd.Cmd {
	return func(args ...string) icmd.Cmd {
		endpoint := []string{"--endpoint-url", endpoint}
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
}

func goBuildS5cmd() func() {
	tmpdir, err := ioutil.TempDir("", "")
	if err != nil {
		panic(err)
	}

	s5cmd := "s5cmd"
	if runtime.GOOS == "windows" {
		s5cmd += ".exe"
	}

	s5cmdPath = filepath.Join(tmpdir, s5cmd)

	workdir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	// 'go build' will change the working directory to the path where tests
	// reside. workdir should be the project root.
	workdir = filepath.Dir(workdir)

	var args []string
	if runtime.GOOS == "windows" {
		/*
		 disable '-race' flag because CI fails with below error.

		 ==2688==ERROR: ThreadSanitizer failed to allocate 0x000001000000
		 (16777216) bytes at 0x040140000000 (error code: 1455)

		 Ref: https://github.com/golang/go/issues/22553
		*/
		args = []string{"build", "-mod=vendor", "-o", s5cmdPath}
	} else {
		args = []string{"build", "-mod=vendor", "-race", "-o", s5cmdPath}
	}
	cmd := exec.Command("go", args...)
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

type ensureOpts struct {
	contentType  *string
	storageClass *string
}

type ensureOption func(*ensureOpts)

func ensureContentType(contentType string) ensureOption {
	return func(opts *ensureOpts) {
		opts.contentType = &contentType
	}
}

func ensureStorageClass(expected string) ensureOption {
	return func(opts *ensureOpts) {
		opts.storageClass = &expected
	}
}

func ensureS3Object(
	client *s3.S3,
	bucket string,
	key string,
	content string,
	fns ...ensureOption,
) error {
	opts := &ensureOpts{}
	for _, fn := range fns {
		fn(opts)
	}

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

	if diff := cmp.Diff(content, body.String()); diff != "" {
		return fmt.Errorf("s3 %v/%v: (-want +got):\n%v", bucket, key, diff)
	}

	if opts.contentType != nil {
		if diff := cmp.Diff(opts.contentType, output.ContentType); diff != "" {
			return fmt.Errorf("content-type of %v/%v: (-want +got):\n%v", bucket, key, diff)
		}
	}

	if opts.storageClass != nil {
		if diff := cmp.Diff(opts.storageClass, output.StorageClass); diff != "" {
			return fmt.Errorf("storage-class of %v/%v: (-want +got):\n%v", bucket, key, diff)
		}
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
	json        bool
	alignment   bool
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

func jsonCheck(v bool) func(*assertOpts) {
	return func(opts *assertOpts) {
		opts.json = v
	}
}

func alignment(v bool) func(*assertOpts) {
	return func(opts *assertOpts) {
		opts.alignment = v
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

	if actual == "" {
		if len(expectedlines) > 0 {
			t.Errorf("expected a content, got empty string")
		}

		return
	}

	// default assertion options
	opts := assertOpts{
		strict:      true,
		sort:        false,
		json:        false,
		alignment:   false,
		trimRegexes: nil,
	}

	for _, fn := range fns {
		fn(&opts)
	}

	// check alignment before trimming spaces
	if opts.alignment {
		if err := checkLineAlignments(actual); err != nil {
			t.Error(err)
		}
	}

	actual = strings.TrimSpace(actual)

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

		// check if each line is json if flag is set
		// multiple structured logs in output should be prevented.
		if opts.json {
			if line != "" && !isJSON(line) {
				t.Errorf("expected a json string for line %q (lineno: %v)", line, i)
			}
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

func checkLineAlignments(actual string) error {
	// use original string. because some characters are
	// trimmed during line preparation and we need to check
	// original string
	actual = strings.TrimSuffix(actual, "\n")
	lines := strings.Split(actual, "\n")

	lineExists := len(lines) > 0
	if !lineExists {
		// nothing to compare
		return nil
	}

	sort.Strings(lines)

	var index int
	for lineno, line := range lines {
		// format:
		// 			2020/03/26 09:14:10          1024.0M 1gb
		//                                  	 	 DIR test/
		//
		// only check the alignment of Dir
		got := strings.LastIndex(line, " ")
		if index == 0 {
			index = got
		}
		if index != got {
			return fmt.Errorf("unaligned string, line: %v expected index: %v, got: %v", lineno, index, got)
		}
	}
	return nil
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

func isJSON(str string) bool {
	var js jsonpkg.RawMessage
	return jsonpkg.Unmarshal([]byte(str), &js) == nil
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

func json(format string, args ...interface{}) compareFunc {
	expected := fmt.Sprintf(format, args...)
	// escape multiline characters
	{
		expected = strings.Replace(expected, "\n", "", -1)
		expected = strings.Replace(expected, "\t", "", -1)
		expected = strings.Replace(expected, "\b", "", -1)
		expected = strings.Replace(expected, " ", "", -1)
		expected = strings.TrimSpace(expected)
	}

	return func(actual string) error {
		if expected == actual {
			return nil
		}

		diff := cmp.Diff(expected, actual)
		return fmt.Errorf("json: (-want +got):\n%v", diff)
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
