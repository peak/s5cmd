package command

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
)

func TestExpandSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		src         map[string][]*storage.Object
		wantObjects []string
		wantError   error
	}{
		{
			name: "merge_multiple_source_urls",
			src: map[string][]*storage.Object{
				"s3://bucket/key": {
					{
						URL: &url.URL{
							Scheme: "s3",
							Bucket: "bucket",
							Path:   "key",
						},
					},
				},
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
							Path:   "dir/a/readme.md",
						},
					},
					{
						URL: &url.URL{
							Scheme: "s3",
							Bucket: "bucket",
							Path:   "dir/b/readme.md",
						},
					},
				},
			},
			wantObjects: []string{
				"s3://bucket/dir/a/readme.md",
				"s3://bucket/dir/b/readme.md",
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
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// t.Parallel()

			srcurls, err := newURLs(keys(tc.src)...)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			client := &storage.MockStorage{}

			for src, objects := range tc.src {
				srcurl, err := url.New(src)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					continue
				}

				ch := generateObjects(objects)

				if src != "s3://bucket/key" {
					client.On("List", mock.Anything, srcurl).Once().Return(ch)
				}
			}

			gotChan := expandSources(context.Background(), client, srcurls...)

			var objects []string
			for obj := range gotChan {
				if obj.Err != nil {
					if obj.Err != tc.wantError {
						t.Errorf("got error = %v, want %v", obj.Err, tc.wantError)
					}
					continue
				}
				objects = append(objects, obj.String())
			}
			// sort read objects
			sort.Strings(objects)
			if !reflect.DeepEqual(objects, tc.wantObjects) {
				t.Errorf("got = %v, want %v", objects, tc.wantObjects)
			}

			client.AssertExpectations(t)
		})
	}
}

func keys(urls map[string][]*storage.Object) []string {
	var urlKeys []string
	for key := range urls {
		urlKeys = append(urlKeys, key)
	}
	return urlKeys
}

func generateObjects(objects []*storage.Object) <-chan *storage.Object {
	ch := make(chan *storage.Object, len(objects))
	go func() {
		defer close(ch)
		for _, object := range objects {
			ch <- object
		}
	}()
	return ch
}
