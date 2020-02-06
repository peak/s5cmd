package url

import (
	"reflect"
	"regexp"
	"testing"
)

func TestHasWild(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want bool
	}{
		{
			name: "string_has_*",
			s:    "s3://a*/b",
			want: true,
		},
		{
			name: "string_has_?",
			s:    "s3://a/?/c",
			want: true,
		},
		{
			name: "string_has_no_wildcard",
			s:    "s3://a/b/c",
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HasWild(tt.s); got != tt.want {
				t.Errorf("HasWild() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseS3Url(t *testing.T) {
	tests := []struct {
		name    string
		object  string
		want    *S3Url
		wantErr bool
	}{
		{
			name:    "error_if_prefix_does_not_match",
			object:  "test/nos3prefix/1.txt",
			wantErr: true,
		},
		{
			name:    "error_if_does_not_have_bucket",
			object:  "s3://",
			wantErr: true,
		},
		{
			name:    "error_if_bucket_name_has_wildcard",
			object:  "s3://a*b",
			wantErr: true,
		},
		{
			name:   "url_with_no_wildcard",
			object: "s3://bucket/key",
			want: &S3Url{
				Key:         "key",
				Bucket:      "bucket",
				Prefix:      "key",
				filterRegex: regexp.MustCompile("^key/.*$"),
				Delimiter:   "/",
			},
		},
		{
			name:   "url_with_no_wildcard_end_with_slash",
			object: "s3://bucket/key/",
			want: &S3Url{
				Key:         "key/",
				Bucket:      "bucket",
				Prefix:      "key/",
				filterRegex: regexp.MustCompile("^key/.*$"),
				Delimiter:   "/",
			},
		},
		{
			name:   "url_with_wildcard",
			object: "s3://bucket/key/a/?/test/*",
			want: &S3Url{
				Key:         "key/a/?/test/*",
				Bucket:      "bucket",
				Prefix:      "key/a/",
				filter:      "?/test/*",
				filterRegex: regexp.MustCompile("^key/a/./test/.*?$"),
				Delimiter:   "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseS3Url(tt.object)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseS3Url() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseS3Url() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestS3Url_setPrefixAndFilter(t *testing.T) {
	tests := []struct {
		name   string
		before *S3Url
		after  *S3Url
	}{
		{
			name: "wild_operation",
			before: &S3Url{
				Key: "a/b_c/*/de/*/test",
			},
			after: &S3Url{
				Key:    "a/b_c/*/de/*/test",
				Prefix: "a/b_c/",
				filter: "*/de/*/test",
			},
		},
		{
			name: "not_wild_operation",
			before: &S3Url{
				Key: "a/b_c/d/e",
			},
			after: &S3Url{
				Key:       "a/b_c/d/e",
				Prefix:    "a/b_c/d/e",
				Delimiter: "/",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.before
			got.setPrefixAndFilter()
			if !reflect.DeepEqual(got, tt.after) {
				t.Errorf("setPrefixAndFilter() got = %v, want %v", got, tt.after)
			}
		})
	}
}

func TestS3Url_ParseS3Url_and_CheckMatch(t *testing.T) {
	tests := []struct {
		name string
		url  string
		keys []string
		want bool
	}{
		{
			name: "match_everything_if_has_no_wildcard_and_prefix",
			url:  "s3://bucket/key",
			keys: []string{
				"key/folder/a",
				"key/test.csv",
				"key/folder/b/test.csv",
				"key/folder/c",
			},
			want: true,
		},
		{
			name: "not_match_if_has_no_wildcard_and_invalid_prefix",
			url:  "s3://bucket/key",
			keys: []string{
				"anotherkey",
				"invalidkey/dummy",
			},
		},
		{
			name: "match_if_has_single_wildcard_and_valid_prefix",
			url:  "s3://bucket/key/?/b",
			keys: []string{
				"key/a/b",
				"key/1/b",
				"key/c/b",
			},
			want: true,
		},
		{
			name: "not_match_if_has_single_wildcard_and_invalid_prefix",
			url:  "s3://bucket/key/?/b",
			keys: []string{
				"another/a/b",
				"invalid/1/b",
			},
		},
		{
			name: "match_if_has_multiple_wildcard_and_valid_prefix",
			url:  "s3://bucket/key/*/b/*/c/*.tsv",
			keys: []string{
				"key/a/b/c/c/file.tsv",
				"key/dummy/b/1/c/file.tsv",
				"key/dummy/b/1/c/another_file.tsv",
				"key/dummy/b/2/c/another_file.tsv",
				"key/a/b/c/c/another_file.tsv",
			},
			want: true,
		},
		{
			name: "not_match_if_has_multiple_wildcard_and_invalid_prefix",
			url:  "s3://bucket/key/*/b/*/c/*.tsv",
			keys: []string{
				"another/a/b/c/c/file.tsv",
				"invalid/dummy/b/1/c/file.tsv",
			},
		},
		{
			name: "not_match_if_multiple_wildcard_not_match_with_key",
			url:  "s3://bucket/prefix/*/c/*.tsv",
			keys: []string{
				"prefix/a/b/c/c/file.bsv",
				"prefix/dummy/a",
			},
		},
		{
			name: "not_match_if_single_wildcard_not_match_with_key",
			url:  "s3://bucket/*.tsv",
			keys: []string{
				"file.bsv",
				"a/b/c.csv",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := ParseS3Url(tt.url)
			if err != nil {
				t.Errorf("unexpected error %v", err)
			}

			for _, key := range tt.keys {
				if got := u.Match(key); got != tt.want {
					t.Errorf("Match() got = %v, want %v", got, tt.want)
				}
			}
		})
	}
}
