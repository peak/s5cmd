package e2e

import (
	"flag"
	"fmt"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/go-cmp/cmp"
	"github.com/iancoleman/strcase"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3bolt"
	"gotest.tools/v3/fs"
	"gotest.tools/v3/icmd"
)

var (
	defaultAccessKeyID     = "s5cmd-test-access-key-id"
	defaultSecretAccessKey = "s5cmd-test-secret-access-key"
)

var (
	flagTestLogLevel = flag.String("test.log.level", "err", "Test log level: {debug|warn|err}")
)

func setup(t *testing.T) (*s3.S3, func(...string) icmd.Cmd, func()) {
	testdir := fs.NewDir(t, t.Name(), fs.WithDir("workdir", fs.WithMode(0700)))
	dbpath := testdir.Join("s3.boltdb")
	workdir := testdir.Join("workdir")

	// we use boltdb as the s3 backend because listing buckets in in-memory
	// backend is not deterministic.
	s3backend, err := s3bolt.NewFile(dbpath)
	if err != nil {
		t.Fatal(err)
	}

	var (
		fakes3LogLevel = *flagTestLogLevel
		awsLogLevel    = aws.LogOff
	)

	switch *flagTestLogLevel {
	case "debug":
		// fakes3 has no 'debug' level. just set to 'info' to get the log we
		// want
		fakes3LogLevel = "info"
		// aws has no level other than 'debug'
		awsLogLevel = aws.LogDebug
	}

	withLogger := gofakes3.WithLogger(
		gofakes3.GlobalLog(
			gofakes3.LogLevel(strings.ToUpper(fakes3LogLevel)),
		),
	)
	faker := gofakes3.New(s3backend, withLogger)
	s3srv := httptest.NewServer(faker.Server())

	s3Config := aws.NewConfig().
		WithEndpoint(s3srv.URL).
		WithRegion("us-east-1").
		WithCredentials(credentials.NewStaticCredentials(defaultAccessKeyID, defaultSecretAccessKey, "")).
		WithDisableSSL(true).
		WithS3ForcePathStyle(true).
		WithLogLevel(awsLogLevel)

	sess := session.New(s3Config)

	s5cmd := func(args ...string) icmd.Cmd {
		endpoint := []string{"-endpoint-url", s3srv.URL}
		args = append(endpoint, args...)

		cmd := icmd.Command("s5cmd", args...)
		cmd.Dir = workdir
		return cmd
	}

	cleanup := func() {
		testdir.Remove()
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
	return strcase.ToKebab(t.Name())
}

func withWorkingDir(dir *fs.Dir) func(*icmd.Cmd) {
	return func(cmd *icmd.Cmd) {
		cmd.Dir = dir.Path()
	}
}

type compareFunc func(string) error

type assertOpts struct {
	strict bool
	sort   bool
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

func assertLines(t *testing.T, actual string, expectedlines map[int]compareFunc, fns ...assertOp) {
	t.Helper()

	// default assertion options
	opts := assertOpts{
		strict: true,
		sort:   false,
	}

	for _, fn := range fns {
		fn(&opts)
	}

	lines := strings.Split(actual, "\n")
	if opts.sort {
		sort.Strings(lines)
	}

	for i, line := range lines {
		// trim consecutive spaces
		line = replaceMatchWithSpace(line, `\s+`)

		// trim last excessive new line. this one does not affect the output
		// testing since it's the last newline character for the shell prompt
		// to start from a new line.
		if i == len(lines)-1 && line == "" {
			continue
		}

		cmp, ok := expectedlines[i]
		if !ok {
			if opts.strict {
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
