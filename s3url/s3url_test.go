package s3url

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

func TestNew(t *testing.T) {
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
				filterRegex: regexp.MustCompile("^key.*$"),
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
			got, err := New(tt.object)
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
				Key:         "a/b_c/*/de/*/test",
				Prefix:      "a/b_c/",
				Delimiter:   "",
				filter:      "*/de/*/test",
				filterRegex: regexp.MustCompile("^a/b_c/.*?/de/.*?/test$"),
			},
		},
		{
			name: "not_wild_operation",
			before: &S3Url{
				Key: "a/b_c/d/e",
			},
			after: &S3Url{
				Key:         "a/b_c/d/e",
				Prefix:      "a/b_c/d/e",
				Delimiter:   "/",
				filter:      "",
				filterRegex: regexp.MustCompile("^a/b_c/d/e.*$"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.before
			if err := got.setPrefixAndFilter(); err != nil {
				t.Errorf("unexpected error %v", err)
			}

			if !reflect.DeepEqual(got, tt.after) {
				t.Errorf("setPrefixAndFilter() got = %v, want %v", got, tt.after)
			}
		})
	}
}

func TestS3Url_New_and_CheckMatch(t *testing.T) {
	tests := []struct {
		name string
		url  string
		keys map[string]string
	}{
		{
			name: "match_only_key_if_has_no_wildcard_and_not_dir_root",
			url:  "s3://bucket/key",
			keys: map[string]string{
				"key": "key",
			},
		},
		{
			name: "match_multiple_if_has_no_wildcard_and_dir_root",
			url:  "s3://bucket/key/",
			keys: map[string]string{
				"key/a/":           "a/",
				"key/test.txt":     "test.txt",
				"key/test.pdf":     "test.pdf",
				"key/test.pdf/aaa": "test.pdf/",
			},
		},
		{
			name: "not_match_if_has_no_wildcard_and_invalid_prefix",
			url:  "s3://bucket/key",
			keys: map[string]string{
				"anotherkey":       "",
				"invalidkey/dummy": "",
			},
		},
		{
			name: "match_if_has_single_wildcard_and_valid_prefix",
			url:  "s3://bucket/key/?/b",
			keys: map[string]string{
				"key/a/b": "a/b",
				"key/1/b": "1/b",
				"key/c/b": "c/b",
			},
		},
		{
			name: "not_match_if_has_single_wildcard_and_invalid_prefix",
			url:  "s3://bucket/key/?/b",
			keys: map[string]string{
				"another/a/b": "",
				"invalid/1/b": "",
			},
		},
		{
			name: "match_if_has_multiple_wildcard_and_valid_prefix",
			url:  "s3://bucket/key/*/b/*/c/*.tsv",
			keys: map[string]string{
				"key/a/b/c/c/file.tsv":             "a/b/c/c/file.tsv",
				"key/dummy/b/1/c/file.tsv":         "dummy/b/1/c/file.tsv",
				"key/dummy/b/1/c/another_file.tsv": "dummy/b/1/c/another_file.tsv",
				"key/dummy/b/2/c/another_file.tsv": "dummy/b/2/c/another_file.tsv",
				"key/a/b/c/c/another_file.tsv":     "a/b/c/c/another_file.tsv",
			},
		},
		{
			name: "not_match_if_has_multiple_wildcard_and_invalid_prefix",
			url:  "s3://bucket/key/*/b/*/c/*.tsv",
			keys: map[string]string{
				"another/a/b/c/c/file.tsv":     "",
				"invalid/dummy/b/1/c/file.tsv": "",
			},
		},
		{
			name: "not_match_if_multiple_wildcard_does_not_match_with_key",
			url:  "s3://bucket/prefix/*/c/*.tsv",
			keys: map[string]string{
				"prefix/a/b/c/c/file.bsv": "",
				"prefix/dummy/a":          "",
			},
		},
		{
			name: "not_match_if_single_wildcard_does_not_match_with_key",
			url:  "s3://bucket/*.tsv",
			keys: map[string]string{
				"file.bsv":  "",
				"a/b/c.csv": "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := New(tt.url)
			if err != nil {
				t.Errorf("unexpected error %v", err)
			}

			for key, want := range tt.keys {
				if got := u.Match(key); got != want {
					t.Errorf("Match() got = %v, want %v", got, want)
				}
			}
		})
	}
}

func Test_parseBatch(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		key    string
		want   string
	}{
		{
			name:   "do_nothing_if_key_does_not_include_prefix",
			prefix: "a/b/c",
			key:    "d/e",
			want:   "d/e",
		},
		{
			name:   "do_nothing_if_prefix_does_not_include_slash",
			prefix: "some_random_string",
			key:    "a/b",
			want:   "a/b",
		},
		{
			name:   "parse_key_if_prefix_is_a_dir",
			prefix: "a/b/",
			key:    "a/b/c/d",
			want:   "c/d",
		},
		{
			name:   "parse_key_if_prefix_is_not_a_dir",
			prefix: "a/b",
			key:    "a/b/asset.txt",
			want:   "b/asset.txt",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseBatch(tt.prefix, tt.key); got != tt.want {
				t.Errorf("parseBatch() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseNonBatch(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		key    string
		want   string
	}{
		{
			name:   "do_nothing_if_key_does_not_include_prefix",
			prefix: "a/b/c",
			key:    "d/e",
			want:   "d/e",
		},
		{
			name:   "do_nothing_if_prefix_equals_to_key",
			prefix: "a/b",
			key:    "a/b",
			want:   "a/b",
		},
		{
			name:   "parse_key_and_return_first_dir_after_prefix",
			prefix: "a/b/",
			key:    "a/b/c/d",
			want:   "c/",
		},
		{
			name:   "parse_key_and_return_asset_after_prefix",
			prefix: "a/b",
			key:    "a/b/asset.txt",
			want:   "asset.txt",
		},
		{
			name:   "parse_key_and_return_current_asset_if_prefix_is_not_dir",
			prefix: "a/b/ab",
			key:    "a/b/abc.txt",
			want:   "abc.txt",
		},
		{
			name:   "parse_key_and_return_current_dir_if_prefix_is_not_dir",
			prefix: "test",
			key:    "testdir/",
			want:   "testdir/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseNonBatch(tt.prefix, tt.key); got != tt.want {
				t.Errorf("parseNonBatch() = %v, want %v", got, tt.want)
			}
		})
	}
}
