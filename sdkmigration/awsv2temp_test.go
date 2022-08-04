package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
	urlpkg "net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func TestS3ImplementsStorageInterface(t *testing.T) {
	var i interface{} = new(S3)
	if _, ok := i.(storage.Storage); !ok {
		t.Errorf("expected %t to implement Storage interface", i)
	}
}

func TestNewSessionPathStyle(t *testing.T) {
	testcases := []struct {
		name            string
		endpoint        urlpkg.URL
		expectPathStyle bool
	}{
		{
			name:            "expect_virtual_host_style_when_missing_endpoint",
			endpoint:        urlpkg.URL{},
			expectPathStyle: false,
		},
		{
			name:            "expect_virtual_host_style_for_transfer_accel",
			endpoint:        urlpkg.URL{Host: transferAccelEndpoint},
			expectPathStyle: false,
		},
		{
			name:            "expect_virtual_host_style_for_google_cloud_storage",
			endpoint:        urlpkg.URL{Host: gcsEndpoint},
			expectPathStyle: false,
		},
		{
			name:            "expect_path_style_for_localhost",
			endpoint:        urlpkg.URL{Host: "127.0.0.1"},
			expectPathStyle: true,
		},
		{
			name:            "expect_path_style_for_custom_endpoint",
			endpoint:        urlpkg.URL{Host: "example.com"},
			expectPathStyle: true,
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			log.Init("trace", false)
			opts := storage.Options{Endpoint: tc.endpoint.Hostname()}
			s3, err := newS3Storage(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			buckets, err := s3.ListBuckets(context.TODO(), "bucket")
			fmt.Println(buckets)
			epu := s3.endpointURL
			fmt.Println("epu", epu)
			bol := true
			//s3.Config.S3ForcePathStyle
			fmt.Println(s3.config.APIOptions)
			got := aws.ToBool(&bol)
			if got != tc.expectPathStyle {
				t.Fatalf("expected: %v, got: %v", tc.expectPathStyle, got)
			}
		})
	}
}

func TestNewSessionWithRegionSetViaEnv(t *testing.T) {

	const expectedRegion = "us-west-2"

	os.Setenv("AWS_REGION", expectedRegion)
	defer os.Unsetenv("AWS_REGION")

	opts := storage.Options{}
	s3, err := newS3Storage(context.Background(), opts)

	if err != nil {
		t.Fatal(err)
	}
	got := s3.config.Region
	s3.config.Region = "us-test-1"
	fmt.Println(s3.config.Region)
	if got != expectedRegion {
		t.Fatalf("expected %v, got %v", expectedRegion, got)
	}
}

func TestNewSessionWithNoSignRequest(t *testing.T) {

	opts := storage.Options{NoSignRequest: true}
	s3, err := newS3Storage(context.Background(), opts)

	if err != nil {
		t.Fatal(err)
	}

	_, gotErr := s3.config.Credentials.Retrieve(context.Background())

	expectedErr := "AnonymousCredentials is not a valid credential provider, and cannot be used to sign AWS requests with"
	//todo search if there is a better way to test this.
	if !strings.Contains(gotErr.Error(), expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, gotErr)
	}
}

func TestNewSessionWithProfileFromFile(t *testing.T) {
	// create a temporary credentials file
	file, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())

	profiles := `[default]
aws_access_key_id = default_profile_key_id
aws_secret_access_key = default_profile_access_key

[p1]
aws_access_key_id = p1_profile_key_id
aws_secret_access_key = p1_profile_access_key

[p2]
aws_access_key_id = p2_profile_key_id
aws_secret_access_key = p2_profile_access_key`

	_, err = file.Write([]byte(profiles))
	if err != nil {
		t.Fatal(err)
	}

	testcases := []struct {
		name               string
		fileName           string
		profileName        string
		expAccessKeyId     string
		expSecretAccessKey string
	}{
		{
			name:               "use default profile",
			fileName:           file.Name(),
			profileName:        "",
			expAccessKeyId:     "default_profile_key_id",
			expSecretAccessKey: "default_profile_access_key",
		},
		{
			name:               "use a non-default profile",
			fileName:           file.Name(),
			profileName:        "p1",
			expAccessKeyId:     "p1_profile_key_id",
			expSecretAccessKey: "p1_profile_access_key",
		},
		{

			name:               "use a non-existent profile",
			fileName:           file.Name(),
			profileName:        "non-existent-profile",
			expAccessKeyId:     "",
			expSecretAccessKey: "",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {

			s3, err := newS3Storage(context.Background(), storage.Options{
				Profile:        tc.profileName,
				CredentialFile: tc.fileName,
			})
			if err != nil {
				t.Fatal(err)
			}

			got, err := s3.config.Credentials.Retrieve(context.Background())
			if err != nil {
				// if there should be such a profile but received an error fail,
				// ignore the error otherwise.
				if tc.expAccessKeyId != "" || tc.expSecretAccessKey != "" {
					t.Fatal(err)
				}
			}

			if got.AccessKeyID != tc.expAccessKeyId || got.SecretAccessKey != tc.expSecretAccessKey {
				t.Errorf("Expected credentials does not match the credential we got!\nExpected: Access Key ID: %v, Secret Access Key: %v\nGot    : Access Key ID: %v, Secret Access Key: %v\n", tc.expAccessKeyId, tc.expSecretAccessKey, got.AccessKeyID, got.SecretAccessKey)
			}
		})
	}
}

func TestS3ListObjects(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := NewMocks3Client(ctrl)
	mockS3 := &S3{
		client: m,
	}

	m.EXPECT().ListObjectsV2(gomock.Any(), gomock.Any()).Return(
		&s3.ListObjectsV2Output{
			CommonPrefixes: []types.CommonPrefix{
				{Prefix: aws.String("key/a/")},
				{Prefix: aws.String("key/b/")},
			},
			Contents: []types.Object{
				{Key: aws.String("key/test.txt")},
				{Key: aws.String("key/test.pdf")},
			},
		}, nil,
	)

	responses := []struct {
		isDir  bool
		url    string
		relurl string
	}{
		{
			isDir:  true,
			url:    "s3://bucket/key/a/",
			relurl: "a/",
		},
		{
			isDir:  true,
			url:    "s3://bucket/key/b/",
			relurl: "b/",
		},
		{
			isDir:  false,
			url:    "s3://bucket/key/test.txt",
			relurl: "test.txt",
		},
		{
			isDir:  false,
			url:    "s3://bucket/key/test.pdf",
			relurl: "test.pdf",
		},
	}

	index := 0
	for got := range mockS3.listObjectsV2(context.Background(), url) {
		if got.Err != nil {
			t.Errorf("unexpected error: %v", got.Err)
			continue
		}
		want := responses[index]
		if diff := cmp.Diff(want.isDir, got.Type.IsDir()); diff != "" {
			t.Errorf("(-want +got):\n%v", diff)
		}
		if diff := cmp.Diff(want.url, got.URL.Absolute()); diff != "" {
			t.Errorf("(-want +got):\n%v", diff)
		}
		if diff := cmp.Diff(want.relurl, got.URL.Relative()); diff != "" {
			t.Errorf("(-want +got):\n%v", diff)
		}
		index++
	}
}

func TestS3ListError(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := NewMocks3Client(ctrl)
	mockS3 := &S3{
		client: m,
	}
	mockErr := fmt.Errorf("mock error")

	m.EXPECT().ListObjectsV2(gomock.Any(), gomock.Any()).Return(
		nil, mockErr,
	)

	for got := range mockS3.listObjectsV2(context.Background(), url) {
		if got.Err != mockErr {
			t.Errorf("error got = %v, want %v", got.Err, mockErr)
		}
	}
}

func TestS3ListNoItemFound(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	m := NewMocks3Client(ctrl)
	mockS3 := &S3{
		client: m,
	}

	m.EXPECT().ListObjectsV2(gomock.Any(), gomock.Any()).Return(
		// output does not include keys that match with given key
		&s3.ListObjectsV2Output{
			CommonPrefixes: []types.CommonPrefix{
				{Prefix: aws.String("anotherkey/a/")},
				{Prefix: aws.String("anotherkey/b/")},
			},
			Contents: []types.Object{
				{Key: aws.String("a/b/c/d/test.txt")},
				{Key: aws.String("unknown/test.pdf")},
			},
		}, nil,
	)

	for got := range mockS3.List(context.Background(), url, true) {
		if got.Err != storage.ErrNoObjectFound {
			t.Errorf("error got = %v, want %v", got.Err, storage.ErrNoObjectFound)
		}
	}
}

func TestS3Retry(t *testing.T) {
	log.Init("debug", false)

	testcases := []struct {
		name          string
		err           error
		expectedRetry int
	}{
		// Internal error
		{
			name:          "InvalidToken",
			err:           awserr.New("InternalError", "internal error", nil),
			expectedRetry: 5,
		},
	}

	//url, err := url.New("s3://bucket/keyyyy")
	//if err != nil {
	//	t.Errorf("unexpected error: %v", err)
	//}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {

			//if err != nil {
			//	t.Fatal(err)
			//}

			ctx := context.Background()

			s33, err := newS3Storage(ctx, storage.Options{MaxRetries: 5})
			m := mockS3{

				s3Client:  s33.client,
				errorCode: "InvalidToken",
			}
			_, err = m.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("bucket"), Key: aws.String("ket")})
			fmt.Println(err)
			if !strings.Contains(err.Error(), strconv.Itoa(tc.expectedRetry)) {
				t.Fatalf("expected: %v, got: %v", tc.expectedRetry, err)
			}

		})
	}

}

type mockS3 struct {
	s3Client  s3Client
	errorCode string
}

func (d *mockS3) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return d.s3Client.GetObject(ctx, params, func(options *s3.Options) {
		options.APIOptions = append(options.APIOptions, d.funcName)
	})
}

func (d *mockS3) funcName(stack *middleware.Stack) error {
	var count int32
	mw := middleware.InitializeMiddlewareFunc("DefaultBucket", func(
		ctx context.Context,
		in middleware.InitializeInput,
		next middleware.InitializeHandler,
	) (
		out middleware.InitializeOutput,
		metadata middleware.Metadata,
		err error,
	) {
		atomic.AddInt32(&count, 1)
		fmt.Println(count)
		return out, metadata, &smithy.GenericAPIError{Code: d.errorCode}
	})
	return stack.Initialize.Add(mw, middleware.Before)
}
