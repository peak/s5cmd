package command

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/peak/s5cmd/mocks"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
)

func Test_expandSources(t *testing.T) {
	tests := []struct {
		name        string
		src         map[string][]*storage.Object
		wantObjects []string
		wantError   error
	}{
		{
			name: "merge_multiple_source_urls",
			src: map[string][]*storage.Object{
				"s3://bucket/key": {},
				"s3://bucket/wildcard/*.txt": {
					{
						URL: &url.URL{
							Scheme: "s3",
							Bucket: "bucket",
							Path:   "wildcard/test.txt",
						},
					},
					{
						URL: &url.URL{
							Scheme: "s3",
							Bucket: "bucket",
							Path:   "wildcard/anothertest.txt",
						},
					},
				},
				"s3://bucket/dir/?/readme.md": {
					{
						URL: &url.URL{
							Scheme: "s3",
							Bucket: "bucket",
							Path:   "dir/subdir1/readme.md",
						},
					},
				},
			},
			wantObjects: []string{
				"s3://bucket/dir/subdir1/readme.md",
				"s3://bucket/key",
				"s3://bucket/wildcard/anothertest.txt",
				"s3://bucket/wildcard/test.txt",
			},
		},
		{
			name: "merge_multiple_with_empty_source",
			src: map[string][]*storage.Object{
				// this source has no item
				"s3://bucket/wildcard/*.txt": {
					{
						Err: storage.ErrNoObjectFound,
					},
				},
				"s3://bucket/*.txt": {
					{
						URL: &url.URL{
							Scheme: "s3",
							Bucket: "bucket",
							Path:   "file1.txt",
						},
					},
					{
						URL: &url.URL{
							Scheme: "s3",
							Bucket: "bucket",
							Path:   "file2.txt",
						},
					},
				},
			},
			wantObjects: []string{
				"s3://bucket/file1.txt",
				"s3://bucket/file2.txt",
			},
		},
		{
			// if multiple source has no item.
			// it will return single storage.ErrNoObjectFound error.
			name: "no_item_found",
			src: map[string][]*storage.Object{
				// this source has no item
				"s3://bucket/wildcard/*.txt": {
					{
						Err: storage.ErrNoObjectFound,
					},
				},
				"s3://bucket/*.txt": {
					{
						Err: storage.ErrNoObjectFound,
					},
				},
			},
			wantError: storage.ErrNoObjectFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srcurls, err := newSources(keys(tt.src)...)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			client := &mocks.Storage{}

			for src, objects := range tt.src {
				srcurl, err := url.New(src)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				if srcurl.HasGlob() {
					// we operate storage.List() only for wildcard operations.
					ch := generateObjects(objects)
					client.On("List", mock.Anything, srcurl, true).Once().Return(ch)
				}
			}

			gotChan := expandSources(context.Background(), client, srcurls...)

			var objects []string
			for obj := range gotChan {
				if obj.Err != nil {
					if obj.Err != tt.wantError {
						t.Errorf("got error = %v, want %v", obj.Err, tt.wantError)
					}
					continue
				}
				objects = append(objects, obj.String())
			}
			// sort read objects
			sort.Strings(objects)
			if !reflect.DeepEqual(objects, tt.wantObjects) {
				t.Errorf("got = %v, want %v", objects, tt.wantObjects)
			}

			client.AssertExpectations(t)
		})
	}
}

func keys(urls map[string][]*storage.Object) []string {
	var urlKeys []string
	for key, _ := range urls {
		urlKeys = append(urlKeys, key)
	}
	return urlKeys
}

func generateObjects(objects []*storage.Object) <-chan *storage.Object {
	ch := make(chan *storage.Object)
	go func() {
		defer close(ch)
		for _, object := range objects {
			ch <- object
		}
	}()
	return ch
}

func Test_checkSources(t *testing.T) {
	tests := []struct {
		name    string
		sources []string
		wantErr bool
	}{
		{
			name: "error_if_local_and_remote_mixed",
			sources: []string{
				"s3://bucket/key",
				"filename.txy",
			},
			wantErr: true,
		},
		{
			name: "error_if_sources_have_bucket",
			sources: []string{
				"s3://bucket/key",
				"s3://bucket",
			},
			wantErr: true,
		},
		{
			name: "error_if_sources_have_s3_prefix",
			sources: []string{
				"s3://bucket/prefix/",
			},
			wantErr: true,
		},
		{
			name: "success",
			sources: []string{
				"s3://bucket/prefix/filename.txt",
				"s3://bucket/wildcard/*.txt",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkSources(tt.sources...); (err != nil) != tt.wantErr {
				t.Errorf("checkSources() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
