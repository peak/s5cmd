package objurl

import (
	"reflect"
	"regexp"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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
			if got := HasGlobCharacter(tt.s); got != tt.want {
				t.Errorf("HasWild() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNew(t *testing.T) {
	tests := []struct {
		name         string
		object       string
		want         *ObjectURL
		wantFilterRe string
		wantErr      bool
	}{
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
			want: &ObjectURL{
				Scheme:    "s3",
				Bucket:    "bucket",
				Path:      "key",
				Prefix:    "key",
				Delimiter: "/",
			},
			wantFilterRe: regexp.MustCompile(`^key.*$`).String(),
		},
		{
			name:   "url_with_no_wildcard_end_with_slash",
			object: "s3://bucket/key/",
			want: &ObjectURL{
				Scheme:    "s3",
				Bucket:    "bucket",
				Path:      "key/",
				Prefix:    "key/",
				Delimiter: "/",
			},
			wantFilterRe: regexp.MustCompile(`^key/.*$`).String(),
		},
		{
			name:   "url_with_wildcard",
			object: "s3://bucket/key/a/?/test/*",
			want: &ObjectURL{
				Scheme:      "s3",
				Bucket:      "bucket",
				Path:        "key/a/?/test/*",
				Prefix:      "key/a/",
				filterRegex: regexp.MustCompile(`^key/a/./test/.*?$`),
				Delimiter:   "",
			},
			wantFilterRe: regexp.MustCompile(`^key/a/./test/.*?$`).String(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.object)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseObjectURL() error = %v, wantErr %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got, cmpopts.IgnoreUnexported(ObjectURL{})); diff != "" {
				t.Errorf("test case %q: ObjectURL mismatch (-want +got):\n%v", tt.name, diff)

			}
			if tt.wantFilterRe != "" {
				if diff := cmp.Diff(tt.wantFilterRe, got.filterRegex.String()); diff != "" {
					t.Errorf("test case %q: ObjectURL.filterRegex mismatch (-want +got):\n%v", tt.name, diff)

				}
			}
		})
	}
}

func TestObjectURL_setPrefixAndFilter(t *testing.T) {
	tests := []struct {
		name   string
		before *ObjectURL
		after  *ObjectURL
	}{
		{
			name: "wild_operation",
			before: &ObjectURL{
				Path: "a/b_c/*/de/*/test",
			},
			after: &ObjectURL{
				Path:        "a/b_c/*/de/*/test",
				Prefix:      "a/b_c/",
				Delimiter:   "",
				filter:      "*/de/*/test",
				filterRegex: regexp.MustCompile("^a/b_c/.*?/de/.*?/test$"),
			},
		},
		{
			name: "not_wild_operation",
			before: &ObjectURL{
				Path: "a/b_c/d/e",
			},
			after: &ObjectURL{
				Path:        "a/b_c/d/e",
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

func TestObjectURL_New_and_CheckMatch(t *testing.T) {
	type matchResult struct {
		matched bool
		relurl  string
	}
	tests := []struct {
		name string
		url  string
		keys map[string]matchResult
	}{
		{
			name: "match_only_key_if_has_no_wildcard_and_not_dir_root",
			url:  "s3://bucket/key",
			keys: map[string]matchResult{
				"key": {true, "key"},
			},
		},
		{
			name: "match_multiple_if_has_no_wildcard_and_dir_root",
			url:  "s3://bucket/key/",
			keys: map[string]matchResult{
				"key/a/":           {true, "a/"},
				"key/test.txt":     {true, "test.txt"},
				"key/test.pdf":     {true, "test.pdf"},
				"key/test.pdf/aaa": {true, "test.pdf/"},
			},
		},
		{
			name: "not_match_if_has_no_wildcard_and_invalid_prefix",
			url:  "s3://bucket/key",
			keys: map[string]matchResult{
				"anotherkey":       {},
				"invalidkey/dummy": {},
			},
		},
		{
			name: "match_if_has_single_wildcard_and_valid_prefix",
			url:  "s3://bucket/key/?/b",
			keys: map[string]matchResult{
				"key/a/b": {true, "a/b"},
				"key/1/b": {true, "1/b"},
				"key/c/b": {true, "c/b"},
			},
		},
		{
			name: "not_match_if_has_single_wildcard_and_invalid_prefix",
			url:  "s3://bucket/key/?/b",
			keys: map[string]matchResult{
				"another/a/b": {},
				"invalid/1/b": {},
			},
		},
		{
			name: "match_if_has_multiple_wildcard_and_valid_prefix",
			url:  "s3://bucket/key/*/b/*/c/*.tsv",
			keys: map[string]matchResult{
				"key/a/b/c/c/file.tsv":             {true, "a/b/c/c/file.tsv"},
				"key/dummy/b/1/c/file.tsv":         {true, "dummy/b/1/c/file.tsv"},
				"key/dummy/b/1/c/another_file.tsv": {true, "dummy/b/1/c/another_file.tsv"},
				"key/dummy/b/2/c/another_file.tsv": {true, "dummy/b/2/c/another_file.tsv"},
				"key/a/b/c/c/another_file.tsv":     {true, "a/b/c/c/another_file.tsv"},
			},
		},
		{
			name: "not_match_if_has_multiple_wildcard_and_invalid_prefix",
			url:  "s3://bucket/key/*/b/*/c/*.tsv",
			keys: map[string]matchResult{
				"another/a/b/c/c/file.tsv":     {},
				"invalid/dummy/b/1/c/file.tsv": {},
			},
		},
		{
			name: "not_match_if_multiple_wildcard_does_not_match_with_key",
			url:  "s3://bucket/prefix/*/c/*.tsv",
			keys: map[string]matchResult{
				"prefix/a/b/c/c/file.bsv": {},
				"prefix/dummy/a":          {},
			},
		},
		{
			name: "not_match_if_single_wildcard_does_not_match_with_key",
			url:  "s3://bucket/*.tsv",
			keys: map[string]matchResult{
				"file.bsv":  {},
				"a/b/c.csv": {},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := New(tt.url)
			if err != nil {
				t.Errorf("unexpected error %v", err)
			}

			for key, matchResult := range tt.keys {
				got := u.Match(key)
				if got != matchResult.matched {
					t.Errorf("Match() got = %v, want %v", got, matchResult.matched)
				}
				if u.Relative() != matchResult.relurl {
					t.Errorf("Match() got = %v, want %v", got, matchResult.relurl)
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
