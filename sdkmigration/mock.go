// Code generated by MockGen. DO NOT EDIT.
// Source: awsv2temp.go

// Package main is a generated GoMock package.
package main

import (
	context "context"
	io "io"
	reflect "reflect"

	manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	gomock "github.com/golang/mock/gomock"
)

// Mocks3Client is a mock of s3Client interface.
type Mocks3Client struct {
	ctrl     *gomock.Controller
	recorder *Mocks3ClientMockRecorder
}

// Mocks3ClientMockRecorder is the mock recorder for Mocks3Client.
type Mocks3ClientMockRecorder struct {
	mock *Mocks3Client
}

// NewMocks3Client creates a new mock instance.
func NewMocks3Client(ctrl *gomock.Controller) *Mocks3Client {
	mock := &Mocks3Client{ctrl: ctrl}
	mock.recorder = &Mocks3ClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *Mocks3Client) EXPECT() *Mocks3ClientMockRecorder {
	return m.recorder
}

// CopyObject mocks base method.
func (m *Mocks3Client) CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, params}
	for _, a := range optFns {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CopyObject", varargs...)
	ret0, _ := ret[0].(*s3.CopyObjectOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CopyObject indicates an expected call of CopyObject.
func (mr *Mocks3ClientMockRecorder) CopyObject(ctx, params interface{}, optFns ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, params}, optFns...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CopyObject", reflect.TypeOf((*Mocks3Client)(nil).CopyObject), varargs...)
}

// CreateBucket mocks base method.
func (m *Mocks3Client) CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, params}
	for _, a := range optFns {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CreateBucket", varargs...)
	ret0, _ := ret[0].(*s3.CreateBucketOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateBucket indicates an expected call of CreateBucket.
func (mr *Mocks3ClientMockRecorder) CreateBucket(ctx, params interface{}, optFns ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, params}, optFns...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateBucket", reflect.TypeOf((*Mocks3Client)(nil).CreateBucket), varargs...)
}

// DeleteBucket mocks base method.
func (m *Mocks3Client) DeleteBucket(ctx context.Context, params *s3.DeleteBucketInput, optFns ...func(*s3.Options)) (*s3.DeleteBucketOutput, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, params}
	for _, a := range optFns {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DeleteBucket", varargs...)
	ret0, _ := ret[0].(*s3.DeleteBucketOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteBucket indicates an expected call of DeleteBucket.
func (mr *Mocks3ClientMockRecorder) DeleteBucket(ctx, params interface{}, optFns ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, params}, optFns...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteBucket", reflect.TypeOf((*Mocks3Client)(nil).DeleteBucket), varargs...)
}

// DeleteObjects mocks base method.
func (m *Mocks3Client) DeleteObjects(arg0 context.Context, arg1 *s3.DeleteObjectsInput, arg2 ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "DeleteObjects", varargs...)
	ret0, _ := ret[0].(*s3.DeleteObjectsOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteObjects indicates an expected call of DeleteObjects.
func (mr *Mocks3ClientMockRecorder) DeleteObjects(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteObjects", reflect.TypeOf((*Mocks3Client)(nil).DeleteObjects), varargs...)
}

// GetObject mocks base method.
func (m *Mocks3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, params}
	for _, a := range optFns {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetObject", varargs...)
	ret0, _ := ret[0].(*s3.GetObjectOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetObject indicates an expected call of GetObject.
func (mr *Mocks3ClientMockRecorder) GetObject(ctx, params interface{}, optFns ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, params}, optFns...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetObject", reflect.TypeOf((*Mocks3Client)(nil).GetObject), varargs...)
}

// HeadBucket mocks base method.
func (m *Mocks3Client) HeadBucket(arg0 context.Context, arg1 *s3.HeadBucketInput, arg2 ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "HeadBucket", varargs...)
	ret0, _ := ret[0].(*s3.HeadBucketOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HeadBucket indicates an expected call of HeadBucket.
func (mr *Mocks3ClientMockRecorder) HeadBucket(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HeadBucket", reflect.TypeOf((*Mocks3Client)(nil).HeadBucket), varargs...)
}

// HeadObject mocks base method.
func (m *Mocks3Client) HeadObject(arg0 context.Context, arg1 *s3.HeadObjectInput, arg2 ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "HeadObject", varargs...)
	ret0, _ := ret[0].(*s3.HeadObjectOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HeadObject indicates an expected call of HeadObject.
func (mr *Mocks3ClientMockRecorder) HeadObject(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HeadObject", reflect.TypeOf((*Mocks3Client)(nil).HeadObject), varargs...)
}

// ListBuckets mocks base method.
func (m *Mocks3Client) ListBuckets(ctx context.Context, params *s3.ListBucketsInput, optFns ...func(*s3.Options)) (*s3.ListBucketsOutput, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, params}
	for _, a := range optFns {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ListBuckets", varargs...)
	ret0, _ := ret[0].(*s3.ListBucketsOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListBuckets indicates an expected call of ListBuckets.
func (mr *Mocks3ClientMockRecorder) ListBuckets(ctx, params interface{}, optFns ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, params}, optFns...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListBuckets", reflect.TypeOf((*Mocks3Client)(nil).ListBuckets), varargs...)
}

// ListObjects mocks base method.
func (m *Mocks3Client) ListObjects(ctx context.Context, params *s3.ListObjectsInput, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, params}
	for _, a := range optFns {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ListObjects", varargs...)
	ret0, _ := ret[0].(*s3.ListObjectsOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListObjects indicates an expected call of ListObjects.
func (mr *Mocks3ClientMockRecorder) ListObjects(ctx, params interface{}, optFns ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, params}, optFns...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListObjects", reflect.TypeOf((*Mocks3Client)(nil).ListObjects), varargs...)
}

// ListObjectsV2 mocks base method.
func (m *Mocks3Client) ListObjectsV2(arg0 context.Context, arg1 *s3.ListObjectsV2Input, arg2 ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ListObjectsV2", varargs...)
	ret0, _ := ret[0].(*s3.ListObjectsV2Output)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListObjectsV2 indicates an expected call of ListObjectsV2.
func (mr *Mocks3ClientMockRecorder) ListObjectsV2(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListObjectsV2", reflect.TypeOf((*Mocks3Client)(nil).ListObjectsV2), varargs...)
}

// SelectObjectContent mocks base method.
func (m *Mocks3Client) SelectObjectContent(ctx context.Context, params *s3.SelectObjectContentInput, optFns ...func(*s3.Options)) (*s3.SelectObjectContentOutput, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, params}
	for _, a := range optFns {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "SelectObjectContent", varargs...)
	ret0, _ := ret[0].(*s3.SelectObjectContentOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SelectObjectContent indicates an expected call of SelectObjectContent.
func (mr *Mocks3ClientMockRecorder) SelectObjectContent(ctx, params interface{}, optFns ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, params}, optFns...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SelectObjectContent", reflect.TypeOf((*Mocks3Client)(nil).SelectObjectContent), varargs...)
}

// Mockdownloader is a mock of downloader interface.
type Mockdownloader struct {
	ctrl     *gomock.Controller
	recorder *MockdownloaderMockRecorder
}

// MockdownloaderMockRecorder is the mock recorder for Mockdownloader.
type MockdownloaderMockRecorder struct {
	mock *Mockdownloader
}

// NewMockdownloader creates a new mock instance.
func NewMockdownloader(ctrl *gomock.Controller) *Mockdownloader {
	mock := &Mockdownloader{ctrl: ctrl}
	mock.recorder = &MockdownloaderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *Mockdownloader) EXPECT() *MockdownloaderMockRecorder {
	return m.recorder
}

// Download mocks base method.
func (m *Mockdownloader) Download(ctx context.Context, w io.WriterAt, input *s3.GetObjectInput, options ...func(*manager.Downloader)) (int64, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, w, input}
	for _, a := range options {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Download", varargs...)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Download indicates an expected call of Download.
func (mr *MockdownloaderMockRecorder) Download(ctx, w, input interface{}, options ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, w, input}, options...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Download", reflect.TypeOf((*Mockdownloader)(nil).Download), varargs...)
}

// Mockuploader is a mock of uploader interface.
type Mockuploader struct {
	ctrl     *gomock.Controller
	recorder *MockuploaderMockRecorder
}

// MockuploaderMockRecorder is the mock recorder for Mockuploader.
type MockuploaderMockRecorder struct {
	mock *Mockuploader
}

// NewMockuploader creates a new mock instance.
func NewMockuploader(ctrl *gomock.Controller) *Mockuploader {
	mock := &Mockuploader{ctrl: ctrl}
	mock.recorder = &MockuploaderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *Mockuploader) EXPECT() *MockuploaderMockRecorder {
	return m.recorder
}

// Upload mocks base method.
func (m *Mockuploader) Upload(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, input}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Upload", varargs...)
	ret0, _ := ret[0].(*manager.UploadOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Upload indicates an expected call of Upload.
func (mr *MockuploaderMockRecorder) Upload(ctx, input interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, input}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Upload", reflect.TypeOf((*Mockuploader)(nil).Upload), varargs...)
}

// MockListObjectsAPIClient is a mock of ListObjectsAPIClient interface.
type MockListObjectsAPIClient struct {
	ctrl     *gomock.Controller
	recorder *MockListObjectsAPIClientMockRecorder
}

// MockListObjectsAPIClientMockRecorder is the mock recorder for MockListObjectsAPIClient.
type MockListObjectsAPIClientMockRecorder struct {
	mock *MockListObjectsAPIClient
}

// NewMockListObjectsAPIClient creates a new mock instance.
func NewMockListObjectsAPIClient(ctrl *gomock.Controller) *MockListObjectsAPIClient {
	mock := &MockListObjectsAPIClient{ctrl: ctrl}
	mock.recorder = &MockListObjectsAPIClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockListObjectsAPIClient) EXPECT() *MockListObjectsAPIClientMockRecorder {
	return m.recorder
}

// ListObjects mocks base method.
func (m *MockListObjectsAPIClient) ListObjects(ctx context.Context, params *s3.ListObjectsInput, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, params}
	for _, a := range optFns {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ListObjects", varargs...)
	ret0, _ := ret[0].(*s3.ListObjectsOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListObjects indicates an expected call of ListObjects.
func (mr *MockListObjectsAPIClientMockRecorder) ListObjects(ctx, params interface{}, optFns ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, params}, optFns...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListObjects", reflect.TypeOf((*MockListObjectsAPIClient)(nil).ListObjects), varargs...)
}
