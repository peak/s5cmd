// Package e2e contains tests that run against a real s5cmd binary,
// compiled on the fly at the start of the test run.
package e2e

import (
	"bytes"
	"context"
	jsonpkg "encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	urlpkg "net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/strutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/google/go-cmp/cmp"
	"github.com/iancoleman/strcase"
	"github.com/igungor/gofakes3"
	"gotest.tools/v3/assert"
	"gotest.tools/v3/fs"
	"gotest.tools/v3/icmd"
)

const (
	// Don't use "race" flag in the build arguments.
	testDisableRaceFlagKey       = "S5CMD_BUILD_BINARY_WITHOUT_RACE_FLAG"
	testDisableRaceFlagVal       = "1"
	s5cmdTestIDEnv               = "S5CMD_ACCESS_KEY_ID"
	s5cmdTestSecretEnv           = "S5CMD_SECRET_ACCESS_KEY"
	s5cmdTestEndpointEnv         = "S5CMD_TEST_ENDPOINT_URL"
	s5cmdTestIsVirtualHost       = "S5CMD_IS_VIRTUAL_HOST"
	s5cmdTestRegionEnv           = "S5CMD_REGION"
	s5cmdTestIKnowWhatImDoingEnv = "S5CMD_I_KNOW_WHAT_IM_DOING"
	maxRetries                   = 10
	deleteObjectsMax             = 1000
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

type setupOpts struct {
	s3backend   string
	endpointURL string
	accessKeyID string
	secretKey   string
	region      string
	timeSource  gofakes3.TimeSource
	enableProxy bool
}

type option func(*setupOpts)

func withS3Backend(backend string) option {
	return func(opts *setupOpts) {
		opts.s3backend = backend
	}
}

func withEndpointURL(url string) option {
	return func(opts *setupOpts) {
		opts.endpointURL = url
	}
}

func withAccessKeyID(key string) option {
	return func(opts *setupOpts) {
		opts.accessKeyID = key
	}
}

func withSecretKey(key string) option {
	return func(opts *setupOpts) {
		opts.secretKey = key
	}
}

func withRegion(region string) option {
	return func(opts *setupOpts) {
		opts.region = region
	}
}

func withTimeSource(timeSource gofakes3.TimeSource) option {
	return func(opts *setupOpts) {
		opts.timeSource = timeSource
	}
}

func withProxy() option {
	return func(opts *setupOpts) {
		opts.enableProxy = true
	}
}

type credentialCfg struct {
	AccessKeyID string
	SecretKey   string
	Region      string
}

func setup(t *testing.T, options ...option) (*s3.Client, func(...string) icmd.Cmd) {
	t.Helper()

	opts := &setupOpts{
		s3backend: "bolt",
	}

	for _, option := range options {
		option(opts)
	}
	testdir, workdir := workdir(t)

	endpoint := ""

	// don't create a local s3 server if tests will run in another endpoint
	if isEndpointFromEnv() {
		endpoint = os.Getenv(s5cmdTestEndpointEnv)
	} else {
		endpoint = server(t, testdir, opts)
	}

	// one of the tests check if s5cmd correctly fails when an incorrect endpoint is given.
	// if test specified an endpoint url, then try to use that url.
	if opts.endpointURL != "" {
		endpoint = opts.endpointURL
	}
	secretKey := ""
	if opts.secretKey != "" {
		secretKey = opts.secretKey
	}

	accessKeyID := ""
	if opts.accessKeyID != "" {
		accessKeyID = opts.accessKeyID
	}

	region := ""
	if opts.region != "" {
		region = opts.accessKeyID
	}

	var cfg *credentialCfg

	if region != "" || accessKeyID != "" || secretKey != "" {
		cfg = &credentialCfg{
			AccessKeyID: accessKeyID,
			SecretKey:   secretKey,
			Region:      region,
		}
	}

	client := s3client(t, storage.Options{
		Endpoint:    endpoint,
		NoVerifySSL: true,
	}, cfg)

	return client, s5cmd(workdir, endpoint)
}

func workdir(t *testing.T) (*fs.Dir, string) {
	// testdir := fs.NewDir() tries to create a new directory which has a
	// prefix = [test function name][operation name]
	// e.g., prefix' = "TestCopySingleS3ObjectToLocal/cp_s3://bucket/object_file"
	// but on windows, directories cannot contain a colon so we replace them
	// with hyphen.
	prefix := t.Name()
	if runtime.GOOS == "windows" {
		prefix = strings.ReplaceAll(prefix, ":", "-")
	}

	testdir := fs.NewDir(t, prefix, fs.WithDir("workdir", fs.WithMode(0700)))
	workdir := testdir.Join("workdir")
	return testdir, workdir
}

func server(t *testing.T, testdir *fs.Dir, opts *setupOpts) string {
	t.Helper()

	s3LogLevel := *flagTestLogLevel

	if *flagTestLogLevel == "debug" {
		s3LogLevel = "info" // aws has no level other than 'debug'
	}

	endpoint := s3ServerEndpoint(t, testdir, s3LogLevel, opts.s3backend, opts.timeSource, opts.enableProxy)

	return endpoint
}

func s3client(t *testing.T, options storage.Options, creds *credentialCfg) *s3.Client {
	t.Helper()

	var logMode aws.ClientLogMode
	if *flagTestLogLevel == "debug" {
		logMode = aws.LogRequestWithBody | aws.LogResponseWithBody
	}

	var id, key, region string

	if creds != nil {
		id = creds.AccessKeyID
		key = creds.SecretKey
		region = creds.Region
	} else {
		id = defaultAccessKeyID
		key = defaultSecretAccessKey
		region = "us-east-1"
	}

	endpoint := options.Endpoint
	isVirtualHost := false
	var retryer func() aws.Retryer

	// get environment variables and use external endpoint url.
	// this can be used to test s3 sources such as GCS, amazon, wasabi etc.
	if isEndpointFromEnv() {
		id = os.Getenv(s5cmdTestIDEnv)
		key = os.Getenv(s5cmdTestSecretEnv)
		endpoint = os.Getenv(s5cmdTestEndpointEnv)
		region = os.Getenv(s5cmdTestRegionEnv)
		retryer = func() aws.Retryer { return newSlowDownRetryer(maxRetries) }
		isVirtualHost = isVirtualHostFromEnv(t)
	}

	cfg := aws.Config{
		Region:        region,
		Credentials:   credentials.NewStaticCredentialsProvider(id, key, ""),
		ClientLogMode: logMode,
	}
	if retryer != nil {
		cfg.Retryer = retryer
	}

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = !isVirtualHost

		// v2 sends a CRC32 checksum with every upload by default, which
		// the fake S3 server used by these tests does not expect. s5cmd
		// itself is configured the same way.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
}

func isVirtualHostFromEnv(t *testing.T) bool {
	isVirtual, err := strconv.ParseBool(os.Getenv(s5cmdTestIsVirtualHost))
	if err != nil {
		t.Fatal(err)
	}
	return isVirtual
}

// slowDownRetryer wraps the SDK's standard retryer, adding a retry for the
// SlowDown code that real S3-compatible services return under load.
type slowDownRetryer struct {
	aws.Retryer
}

func newSlowDownRetryer(maxRetries int) aws.Retryer {
	return &slowDownRetryer{
		Retryer: retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = maxRetries + 1
		}),
	}
}

func (c *slowDownRetryer) IsErrorRetryable(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "SlowDown" {
		return true
	}

	return c.Retryer.IsErrorRetryable(err)
}

func isEndpointFromEnv() bool {
	return os.Getenv(s5cmdTestIDEnv) != "" &&
		os.Getenv(s5cmdTestSecretEnv) != "" &&
		os.Getenv(s5cmdTestEndpointEnv) != "" &&
		os.Getenv(s5cmdTestRegionEnv) != "" &&
		os.Getenv(s5cmdTestIsVirtualHost) != "" &&
		os.Getenv(s5cmdTestIKnowWhatImDoingEnv) == "1"
}

// skip the test if testing with google endpoint.
func skipTestIfGCS(t *testing.T, format string) {
	endpoint, err := urlpkg.Parse(os.Getenv(s5cmdTestEndpointEnv))
	if err != nil {
		t.Fatal(err)
	}

	if storage.IsGoogleEndpoint(*endpoint) {
		t.Skip(format)
	}
}

func s5cmd(workdir, endpoint string) func(args ...string) icmd.Cmd {
	return func(args ...string) icmd.Cmd {
		endpoint := []string{"--endpoint-url", endpoint}
		args = append(endpoint, args...)

		cmd := icmd.Command(s5cmdPath, args...)
		env := os.Environ()

		id := defaultAccessKeyID
		secret := defaultSecretAccessKey

		if isEndpointFromEnv() {
			id = os.Getenv(s5cmdTestIDEnv)
			secret = os.Getenv(s5cmdTestSecretEnv)
			env = append(
				env,
				[]string{
					fmt.Sprintf("AWS_REGION=%v", os.Getenv(s5cmdTestRegionEnv)),
				}...,
			)
		}

		env = append(
			env,
			[]string{
				fmt.Sprintf("AWS_ACCESS_KEY_ID=%v", id),
				fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%v", secret),
			}...,
		)
		cmd.Env = env
		cmd.Dir = workdir
		// increase timeout for slow CI
		cmd.Timeout = 10 * time.Minute
		return cmd
	}
}

func goBuildS5cmd() func() {
	tmpdir, err := os.MkdirTemp("", "")
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

	if os.Getenv(testDisableRaceFlagKey) == testDisableRaceFlagVal {
		/*
		 1. disable '-race' flag because CI fails with below error.

		 ==2688==ERROR: ThreadSanitizer failed to allocate 0x000001000000
		 (16777216) bytes at 0x040140000000 (error code: 1455)

		 Ref: https://github.com/golang/go/issues/22553

		 2.  Some distributions default to buildmode pie which is incompatible with race flag.

		 Ref: Alpine Linux: "All userland binaries are compiled as Position
		 Independent Executables (PIE)..." https://www.alpinelinux.org/about/

		 Ref 2: "-buildmode=pie not supported when -race is enabled"
		 https://cs.opensource.google/go/go/+/master:src/cmd/go/internal/work/init.go;l=245;drc=eaf21256545ae04a35fa070763faa6eb2098591d
		*/
		args = []string{"build", "-o", s5cmdPath}
	} else {
		args = []string{"build", "-race", "-o", s5cmdPath}
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

func createBucket(t *testing.T, client *s3.Client, bucket string) {
	t.Helper()

	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
		ACL:    types.BucketCannedACLPublicRead,
	}

	_, err := client.CreateBucket(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}

	if !isEndpointFromEnv() {
		return
	}

	t.Cleanup(func() {
		// cleanup if bucket exists.
		_, err := client.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: aws.String(bucket)})
		if err == nil {

			listInput := s3.ListObjectsInput{
				Bucket: aws.String(bucket),
			}

			//remove objects first.
			// delete each object individually if using GCS.
			// v2 has no ListObjectsPages; the marker is advanced by hand
			// because it also ships no paginator for the legacy operation.
			if isGoogleEndpointFromEnv(t) {
				for {
					page, err := client.ListObjects(context.Background(), &listInput)
					if err != nil {
						t.Fatal(err)
					}
					for _, c := range page.Contents {
						//nolint:errcheck // best-effort cleanup
						client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
							Bucket: aws.String(bucket),
							Key:    c.Key,
						})
					}
					if !aws.ToBool(page.IsTruncated) {
						break
					}
					marker := aws.ToString(page.NextMarker)
					if marker == "" && len(page.Contents) > 0 {
						marker = aws.ToString(page.Contents[len(page.Contents)-1].Key)
					}
					if marker == "" {
						break
					}
					listInput.Marker = aws.String(marker)
				}
			}

			chunkSize := deleteObjectsMax

			var keys []types.ObjectIdentifier
			initKeys := func() {
				keys = make([]types.ObjectIdentifier, 0)
			}

			deleteChunk := func() {
				_, err := client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
					Bucket: aws.String(bucket),
					Delete: &types.Delete{Objects: keys},
				})
				if err != nil {
					t.Fatal(err)
				}
				initKeys()
			}

			listVersionsInput := s3.ListObjectVersionsInput{
				Bucket: aws.String(bucket),
			}

			versionPager := s3.NewListObjectVersionsPaginator(client, &listVersionsInput)
			for versionPager.HasMorePages() {
				page, err := versionPager.NextPage(context.Background())
				if err != nil {
					t.Fatal(err)
				}

				for _, v := range page.Versions {
					keys = append(keys, types.ObjectIdentifier{
						Key:       v.Key,
						VersionId: v.VersionId,
					})
					if len(keys) == chunkSize {
						deleteChunk()
					}
				}

				for _, d := range page.DeleteMarkers {
					keys = append(keys, types.ObjectIdentifier{
						Key:       d.Key,
						VersionId: d.VersionId,
					})
					if len(keys) == chunkSize {
						deleteChunk()
					}
				}
			}

			if len(keys) > 0 {
				deleteChunk()
			}
			// delete bucket after.
			_, err = client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
				Bucket: aws.String(bucket),
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	})

}

func isGoogleEndpointFromEnv(t *testing.T) bool {
	endpoint, err := urlpkg.Parse(os.Getenv(s5cmdTestEndpointEnv))
	if err != nil {
		t.Fatal(err)
	}
	return storage.IsGoogleEndpoint(*endpoint)
}

func setBucketVersioning(t *testing.T, s3client *s3.Client, bucket string, versioning string) {
	t.Helper()
	_, err := s3client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatus(versioning),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

var errS3NoSuchKey = fmt.Errorf("s3: no such key")

type ensureOpts struct {
	cacheControl       *string
	expires            *string
	storageClass       *string
	contentType        *string
	contentDisposition *string
	contentEncoding    *string
	encryptionMethod   *string
	encryptionKeyID    *string
	metadata           map[string]*string
}

type ensureOption func(*ensureOpts)

func ensureCacheControl(cacheControl string) ensureOption {
	return func(opts *ensureOpts) {
		opts.cacheControl = &cacheControl
	}
}

func ensureExpires(expires string) ensureOption {
	return func(opts *ensureOpts) {
		opts.expires = &expires
	}
}

func ensureStorageClass(expected string) ensureOption {
	return func(opts *ensureOpts) {
		opts.storageClass = &expected
	}
}

func ensureContentType(contentType string) ensureOption {
	return func(opts *ensureOpts) {
		opts.contentType = &contentType
	}
}

func ensureContentDisposition(contentDisposition string) ensureOption {
	return func(opts *ensureOpts) {
		opts.contentDisposition = &contentDisposition
	}
}

func ensureContentEncoding(contentEncoding string) ensureOption {
	return func(opts *ensureOpts) {
		opts.contentEncoding = &contentEncoding
	}
}
func ensureEncryptionMethod(encryptionMethod string) ensureOption {
	return func(opts *ensureOpts) {
		opts.encryptionMethod = &encryptionMethod
	}
}

func ensureEncryptionKeyID(encryptionKeyID string) ensureOption {
	return func(opts *ensureOpts) {
		opts.encryptionKeyID = &encryptionKeyID
	}
}
func ensureArbitraryMetadata(metadata map[string]*string) ensureOption {
	return func(opts *ensureOpts) {
		opts.metadata = metadata
	}
}

func ensureS3Object(
	client *s3.Client,
	bucket string,
	key string,
	content string,
	fns ...ensureOption,
) error {
	opts := &ensureOpts{}
	for _, fn := range fns {
		fn(opts)
	}

	output, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey" {
		return fmt.Errorf("%v: %w", key, errS3NoSuchKey)
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

	if opts.cacheControl != nil {
		if diff := cmp.Diff(opts.cacheControl, output.CacheControl); diff != "" {
			return fmt.Errorf("cache-control of %v/%v: (-want +got):\n%v", bucket, key, diff)
		}
	}

	if opts.expires != nil {
		// v1 surfaced the raw Expires header as a *string; in v2 the
		// parsed Expires field is deprecated in favour of ExpiresString,
		// which is the raw header again.
		if diff := cmp.Diff(*opts.expires, aws.ToString(output.ExpiresString)); diff != "" {
			return fmt.Errorf("expires of %v/%v: (-want +got):\n%v", bucket, key, diff)
		}
	}

	if opts.contentEncoding != nil {
		if diff := cmp.Diff(opts.contentEncoding, output.ContentEncoding); diff != "" {
			return fmt.Errorf("content-encoding of %v/%v: (-want +got):\n%v", bucket, key, diff)
		}
	}

	if opts.contentType != nil {
		if diff := cmp.Diff(opts.contentType, output.ContentType); diff != "" {
			return fmt.Errorf("content-type of %v/%v: (-want +got):\n%v", bucket, key, diff)
		}
	}

	if opts.contentDisposition != nil {
		if diff := cmp.Diff(opts.contentDisposition, output.ContentDisposition); diff != "" {
			return fmt.Errorf("content-disposition of %v/%v: (-want +got):\n%v", bucket, key, diff)
		}

	}

	if opts.storageClass != nil {
		// v2 models storage class as a string enum rather than a *string,
		// and S3 omits the header entirely for STANDARD.
		storageClassofOutput := aws.String("STANDARD")
		if output.StorageClass != "" {
			storageClassofOutput = aws.String(string(output.StorageClass))
		}

		if diff := cmp.Diff(opts.storageClass, storageClassofOutput); diff != "" {
			return fmt.Errorf("storage-class of %v/%v: (-want +got):\n%v", bucket, key, diff)
		}
	}

	if opts.encryptionMethod != nil {
		// v2 models server-side encryption as a string enum, not a *string
		if diff := cmp.Diff(*opts.encryptionMethod, string(output.ServerSideEncryption)); diff != "" {
			return fmt.Errorf("encryption-method of %v/%v: (-want +got):\n%v", bucket, key, diff)
		}
	}

	if opts.encryptionKeyID != nil {
		if diff := cmp.Diff(opts.encryptionKeyID, output.SSEKMSKeyId); diff != "" {
			return fmt.Errorf("encryption-key-id of %v/%v: (-want +got):\n%v", bucket, key, diff)
		}
	}

	if opts.metadata != nil {
		// v2 models user metadata as map[string]string rather than
		// map[string]*string, and reports the keys lowercased where v1
		// canonicalised them ("Key1"). S3 metadata keys are
		// case-insensitive, so compare them that way.
		lowered := make(map[string]string, len(output.Metadata))
		for k, v := range output.Metadata {
			lowered[strings.ToLower(k)] = v
		}

		for mkey := range opts.metadata {
			got, ok := lowered[strings.ToLower(mkey)]
			if opts.metadata[mkey] == nil || !ok {
				return fmt.Errorf("check the assertion keys of %v/%v key:%v\n", bucket, key, mkey)
			}
			if diff := cmp.Diff(*opts.metadata[mkey], got); diff != "" {
				return fmt.Errorf("arbitrary metadata of %v/%v: (-want +got):\n%v", bucket, key, diff)
			}
		}
	}
	return nil
}

type putOption func(*s3.PutObjectInput)

func putArbitraryMetadata(metadata map[string]*string) putOption {
	return func(opts *s3.PutObjectInput) {
		m := make(map[string]string, len(metadata))
		for k, v := range metadata {
			m[k] = aws.ToString(v)
		}
		opts.Metadata = m
	}
}

func putStorageClass(storageClass string) putOption {
	return func(opts *s3.PutObjectInput) {
		opts.StorageClass = types.StorageClass(storageClass)
	}
}

func putFile(t *testing.T, client *s3.Client, bucket string, filename string, content string, opts ...putOption) {
	t.Helper()
	input := &s3.PutObjectInput{
		Body:   strings.NewReader(content),
		Bucket: aws.String(bucket),
		Key:    aws.String(filename),
	}

	for _, opt := range opts {
		opt(input)
	}

	_, err := client.PutObject(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
}

func replaceMatchWithSpace(input string, match ...string) string {
	for _, m := range match {
		if m == "" {
			continue
		}
		re := regexp.MustCompile(strutil.AddNewLineFlag(m))
		input = re.ReplaceAllString(input, " ")
	}

	return input
}

func s3BucketFromTestNameWithPrefix(t *testing.T, prefix string) string {
	t.Helper()
	bucket := strcase.ToKebab(t.Name())

	reg, _ := regexp.Compile("[^-a-z0-9]+")

	if prefix != "" {
		bucket = fmt.Sprintf("%v-%v", prefix, bucket)
	}

	bucket = reg.ReplaceAllString(bucket, "")

	return addRandomSuffixTo(bucket)
}

func TestS3BucketFromTestNameWithPrefix(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		name          string
		prefix        string
		expectedRegex string
	}{
		{
			name:          "./*?",
			prefix:        "",
			expectedRegex: "test-s-3-bucket-from-test-name-with-prefix-.{7}$",
		},
		{
			name:          "don't_use_",
			prefix:        "",
			expectedRegex: "test-s-3-bucket-from-test-name-with-prefix-.{7}$",
		},
		{
			name:          "pref",
			prefix:        "pref",
			expectedRegex: "pref-test-s-3-bucket-from-test-name-with-p-.{7}$",
		},
		{
			name:          "chars",
			prefix:        "./*?",
			expectedRegex: "test-s-3-bucket-from-test-name-with-prefi-.{7}$",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// adds tc.name as suffix to "TestS3BucketFromTestNameWithPrefix" and then modifies it.
			result := s3BucketFromTestNameWithPrefix(t, tc.prefix)

			assertLines(t, result, map[int]compareFunc{
				0: match(tc.expectedRegex),
			})
		})

	}
}

func s3BucketFromTestName(t *testing.T) string {
	t.Helper()
	return s3BucketFromTestNameWithPrefix(t, "")
}

// addRandomSuffixTo appends random 7 characters to given bucket name.
// If bucket name is longer than 50 chars, trims it down to 50 chars.
func addRandomSuffixTo(bucketName string) string {
	bucketName = fmt.Sprintf("%v-%v", bucketName, randomString(7))

	if len(bucketName) > 50 {
		bucketName = fmt.Sprintf("%v-%v", bucketName[:42], randomString(7))
	}

	return bucketName
}

func TestAddRandomSuffixTo(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		name          string
		bucketName    string
		expectedRegex string
	}{
		{
			name:          "shorter-than-50-chars",
			bucketName:    "TestName",
			expectedRegex: "TestName-.{7}$",
		},
		{
			name:          "between-42-and-50-chars",
			bucketName:    "ThisTestStringIsSupposedToBeInBetween42And50Chars",
			expectedRegex: "ThisTestStringIsSupposedToBeInBetween42And-.{7}$",
		},
		{
			name:          "longer-than-50-chars",
			bucketName:    "ThisTestStringIsSupposedToBeMuchMuchLongerThanFiftyCharacters",
			expectedRegex: "ThisTestStringIsSupposedToBeMuchMuchLonger-.{7}$",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			result := addRandomSuffixTo(tc.bucketName)

			assert.Assert(t, len(result) <= 63)

			assertLines(t, result, map[int]compareFunc{
				0: match(tc.expectedRegex),
			})
		})

	}
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

func withEnv(key, value string) func(*icmd.Cmd) {
	return func(cmd *icmd.Cmd) {
		if i := indexSlice(cmd.Env, key+"=", strings.HasPrefix); i > 0 {
			cmd.Env[i] = key + "=" + value
		} else {
			cmd.Env = append(cmd.Env, key+"="+value)
		}
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
	re := regexp.MustCompile(strutil.AddNewLineFlag(match))
	return func(opts *assertOpts) {
		opts.trimRegexes = append(opts.trimRegexes, re)
	}
}

func assertError(t *testing.T, err error, expected any) {
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
	re := regexp.MustCompile(strutil.AddNewLineFlag(expected))
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

func equals(format string, args ...any) compareFunc {
	expected := fmt.Sprintf(format, args...)
	return func(actual string) error {
		if expected == actual {
			return nil
		}

		diff := cmp.Diff(expected, actual)
		return fmt.Errorf("equals: (-want +got):\n%v", diff)
	}
}

func json(format string, args ...any) compareFunc {
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

func prefix(format string, args ...any) compareFunc {
	expected := fmt.Sprintf(format, args...)
	return func(actual string) error {
		if strings.HasPrefix(actual, expected) {
			return nil
		}

		diff := cmp.Diff(expected, actual)
		return fmt.Errorf("prefix: (-want +got):\n%v", diff)
	}
}

func suffix(format string, args ...any) compareFunc {
	expected := fmt.Sprintf(format, args...)
	return func(actual string) error {
		if strings.HasSuffix(actual, expected) {
			return nil
		}

		diff := cmp.Diff(expected, actual)
		return fmt.Errorf("suffix: (-want +got):\n%v", diff)
	}
}

func contains(format string, args ...any) compareFunc {
	expected := fmt.Sprintf(format, args...)
	return func(actual string) error {
		if strings.Contains(actual, expected) {
			return nil
		}

		diff := cmp.Diff(expected, actual)
		return fmt.Errorf("contains: (-want +got):\n%v", diff)
	}
}

func newFixedTimeSource(t time.Time) *fixedTimeSource {
	return &fixedTimeSource{time: t}
}

type fixedTimeSource struct {
	mu   sync.Mutex
	time time.Time
}

func (l *fixedTimeSource) Now() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.time
}

func (l *fixedTimeSource) Since(t time.Time) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.time.Sub(t)
}

func (l *fixedTimeSource) Advance(by time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.time = l.time.Add(by)
}

func indexSlice(slice []string, target string, fn func(str, target string) bool) int {
	for i, str := range slice {
		if fn(str, target) {
			return i
		}
	}
	return -1
}
