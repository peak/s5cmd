package url

import (
	"path/filepath"
	"reflect"
	"regexp"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/peak/s5cmd/strutil"
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
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := hasGlobCharacter(tc.s); got != tc.want {
				t.Errorf("HasWild() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNew(t *testing.T) {
	tests := []struct {
		name         string
		object       string
		want         *URL
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
			want: &URL{
				Scheme:    "s3",
				Bucket:    "bucket",
				Path:      "key",
				Prefix:    "key",
				Delimiter: "/",
			},
			wantFilterRe: regexp.MustCompile(strutil.AddNewLineFlag(`^key.*$`)).String(),
		},
		{
			name:   "url_with_no_wildcard_end_with_slash",
			object: "s3://bucket/key/",
			want: &URL{
				Scheme:    "s3",
				Bucket:    "bucket",
				Path:      "key/",
				Prefix:    "key/",
				Delimiter: "/",
			},
			wantFilterRe: regexp.MustCompile(strutil.AddNewLineFlag(`^key/.*$`)).String(),
		},
		{
			name:   "url_with_wildcard",
			object: "s3://bucket/key/a/?/test/*",
			want: &URL{
				Scheme:      "s3",
				Bucket:      "bucket",
				Path:        "key/a/?/test/*",
				Prefix:      "key/a/",
				filterRegex: regexp.MustCompile(strutil.AddNewLineFlag(`^key/a/./test/.*$`)),
				Delimiter:   "",
			},
			wantFilterRe: regexp.MustCompile(strutil.AddNewLineFlag(`^key/a/./test/.*$`)).String(),
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := New(tc.object)
			if (err != nil) != tc.wantErr {
				t.Errorf("ParseURL() error = %v, wantErr %v", err, tc.wantErr)
			}
			if diff := cmp.Diff(tc.want, got, cmpopts.IgnoreUnexported(URL{})); diff != "" {
				t.Errorf("test case %q: URL mismatch (-want +got):\n%v", tc.name, diff)

			}
			if tc.wantFilterRe != "" {
				if diff := cmp.Diff(tc.wantFilterRe, got.filterRegex.String()); diff != "" {
					t.Errorf("test case %q: URL.filterRegex mismatch (-want +got):\n%v", tc.name, diff)
				}
			}
		})
	}
}

func TestJoin(t *testing.T) {
	tests := []struct {
		name       string
		before     *URL
		objectName string
		after      *URL
	}{
		// URL is remote, expected to keep adjacent slashes
		{
			name: "remote:url_with_adjacent_slashes",
			before: &URL{
				Path: "s3://bucket/a//b/",
				Type: remoteObject,
			},
			objectName: "test.txt",
			after: &URL{
				Path: "s3://bucket/a//b/test.txt",
				Type: remoteObject,
			},
		},
		{
			name: "remote:objectName_has_adjacent_slashes",
			before: &URL{
				Path: "s3://bucket/a/b/",
				Type: remoteObject,
			},
			objectName: "folder//test.txt",
			after: &URL{
				Path: "s3://bucket/a/b/folder//test.txt",
				Type: remoteObject,
			},
		},
		{
			name: "remote:objectName_url_has_adjacent_slashes",
			before: &URL{
				Path: "s3://bucket/a//b/",
				Type: remoteObject,
			},
			objectName: "/folder//test.txt",
			after: &URL{
				Path: "s3://bucket/a//b//folder//test.txt",
				Type: remoteObject,
			},
		},
		// URL is local, expected to clean adjacent slashes
		{
			name: "local:url_with_adjacent_slashes",
			before: &URL{
				Path: "dir/a//b/",
				Type: localObject,
			},
			objectName: "test.txt",
			after: &URL{
				Path: "dir/a/b/test.txt",
				Type: localObject,
			},
		},
		{
			name: "local:objectName_has_adjacent_slashes",
			before: &URL{
				Path: "dir/a/b/",
				Type: localObject,
			},
			objectName: "folder//test.txt",
			after: &URL{
				Path: "dir/a/b/folder/test.txt",
				Type: localObject,
			},
		},
		{
			name: "local:objectName_url_has_adjacent_slashes",
			before: &URL{
				Path: "dir/a//b/",
				Type: localObject,
			},
			objectName: "/folder//test.txt",
			after: &URL{
				Path: "dir/a/b/folder/test.txt",
				Type: localObject,
			},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := tc.before.Join(tc.objectName)
			if !reflect.DeepEqual(got, tc.after) {
				t.Errorf("Join() got = %v, want %v", got, tc.after)
			}
		})
	}
}

func TestURLSetPrefixAndFilter(t *testing.T) {
	tests := []struct {
		name   string
		before *URL
		after  *URL
	}{
		{
			name: "wild_operation",
			before: &URL{
				Path: "a/b_c/*/de/*/test",
			},
			after: &URL{
				Path:        "a/b_c/*/de/*/test",
				Prefix:      "a/b_c/",
				Delimiter:   "",
				filter:      "*/de/*/test",
				filterRegex: regexp.MustCompile(strutil.AddNewLineFlag("^a/b_c/.*/de/.*/test$")),
			},
		},
		{
			name: "not_wild_operation",
			before: &URL{
				Path: "a/b_c/d/e",
			},
			after: &URL{
				Path:        "a/b_c/d/e",
				Prefix:      "a/b_c/d/e",
				Delimiter:   "/",
				filter:      "",
				filterRegex: regexp.MustCompile(strutil.AddNewLineFlag("^a/b_c/d/e.*$")),
			},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := tc.before
			if err := got.setPrefixAndFilter(); err != nil {
				t.Errorf("unexpected error %v", err)
			}

			if !reflect.DeepEqual(got, tc.after) {
				t.Errorf("setPrefixAndFilter() got = %v, want %v", got, tc.after)
			}
		})
	}
}

func TestCheckMatch(t *testing.T) {
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
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			u, err := New(tc.url)
			if err != nil {
				t.Errorf("unexpected error %v", err)
			}

			for key, matchResult := range tc.keys {
				got := u.Match(key)
				if got != matchResult.matched {
					t.Errorf("Match() got = %v, want %v", got, matchResult.matched)
				}
				if got && u.Relative() != matchResult.relurl {
					t.Errorf("Match() got = %v, want %v", got, matchResult.relurl)
				}
			}
		})
	}
}

func TestParseBatch(t *testing.T) {
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
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := parseBatch(tc.prefix, tc.key); got != tc.want {
				t.Errorf("parseBatch() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestParseNonBatch(t *testing.T) {
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
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := parseNonBatch(tc.prefix, tc.key); got != tc.want {
				t.Errorf("parseNonBatch() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestURLIsBucket(t *testing.T) {
	tests := []struct {
		input     string
		want      bool
		wantError bool
	}{
		{"s3://bucket", true, false},
		{"s3://bucket/file", false, false},
		{"bucket", false, false},
		{"s3://", false, true},
	}
	for _, tc := range tests {
		url, err := New(tc.input)
		if tc.wantError && err != nil {
			continue
		}

		if tc.wantError && err == nil {
			t.Errorf("expecting error for input %s", tc.input)
		}

		if err != nil {
			t.Errorf("unexpected error: %v for input %s", err, tc.input)
			continue
		}

		if url.IsBucket() != tc.want {
			t.Errorf("isBucket should return %v for  %s", tc.want, tc.input)
		}
	}
}

func TestURLWithMode(t *testing.T) {
	tests := []struct {
		input          string
		raw            bool
		prefixExpected string
		filterExpected string
	}{
		{"s3://bucket/file*.txt", false, "file", "*.txt"},
		{"s3://bucket/file*.txt", true, "", ""},
		{"s3://bucket/abc/deneme*.txt", false, "abc/deneme", "*.txt"},
		{"s3://bucket/abc/deneme*.txt", true, "", ""},
		{"deneme*.txt", false, "deneme", "*.txt"},
		{"deneme*.txt", true, "", ""},
	}
	for _, tc := range tests {
		url, err := New(tc.input, WithRaw(tc.raw))
		if err != nil {
			t.Errorf("There is an error in %s\n", tc.input)
		}

		if url.Prefix != tc.prefixExpected {
			t.Errorf("%s : url prefix %s does not match with expected %s\n", tc.input, url.Prefix, tc.prefixExpected)
		}

		if url.filter != tc.filterExpected {
			t.Errorf("%s: url filter %s does not match with expected filter %s\n", tc.input, url.Prefix, tc.filterExpected)
		}
	}
}

func TestURLSetRelative(t *testing.T) {
	type testcase struct {
		name   string
		base   string
		target string
		expect string
	}

	sep := string(filepath.Separator)
	tests := []testcase{
		{
			name:   "local_sibling_child_object",
			base:   sep + "parent" + sep + "child" + sep + "object",
			target: sep + "parent" + sep + "child2" + sep + "object",
			expect: ".." + sep + "child2" + sep + "object",
		},
		{
			name:   "local_same_directory_object",
			base:   sep + "parent" + sep + "child" + sep + "object",
			target: sep + "parent" + sep + "child" + sep + "object2",
			expect: "object2",
		},
		{
			name:   "s3_sibling_child_object",
			base:   "s3://bucket/parent" + sep + "child" + sep + "object",
			target: "s3://bucket/parent" + sep + "child2" + sep + "",
			expect: ".." + sep + "child2",
		},
		{
			name:   "local_child_directory_fully_wildcarded",
			base:   sep + "parent" + sep + "*" + sep + "object",
			target: sep + "parent" + sep + "child" + sep + "object",
			expect: "child" + sep + "object",
		},
		{
			name:   "local_child_directory_partially_wildcarded",
			base:   sep + "parent" + sep + "c*d" + sep + "object",
			target: sep + "parent" + sep + "child" + sep + "object",
			expect: "child" + sep + "object",
		},
		{
			name:   "local_child_directory_fully_wildcarded_with_question_mark",
			base:   sep + "parent" + sep + "?" + sep + "object",
			target: sep + "parent" + sep + "c" + sep + "object",
			expect: "c" + sep + "object",
		},
		{
			name:   "s3_child_directory_wildcarded",
			base:   "s3://bucket/parent" + sep + "*" + sep + "object",
			target: "s3://bucket/parent" + sep + "child2" + sep + "",
			expect: "child2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseURL, err := New(tt.base)
			if err != nil {
				t.Errorf("URL cannot be instantiated: \nPath: %v, Error: %v", tt.base, err)
			}
			targUrl, err := New(tt.target)
			if err != nil {
				t.Errorf("URL cannot be instantiated:\nPath: %v, Error: %v", tt.base, err)
			}

			targUrl.SetRelative(baseURL)

			if diff := cmp.Diff(tt.expect, targUrl.Relative()); diff != "" {
				t.Errorf("SetRelative() with %s did not produce expected path (-want +got):\n%s", tt.name, diff)
			}
		})
	}
}

func TestToFromBytes(t *testing.T) {
	testcases := []struct {
		name     string
		key      string
		relative string
	}{
		{
			name:     "plain remote",
			key:      "s3://bucket/file",
			relative: "file",
		},
		{
			name:     "space char remote",
			key:      "s3://bucket/s ace/file",
			relative: "s ace/file",
		},
		{
			name:     "space char remote",
			key:      "s3://bucket/li\ne/file",
			relative: "li\ne/file",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			url, err := New(tc.key)
			if err != nil {
				t.Errorf("URL cannot be instantiated: \nPath: %v, Error: %v", tc.key, err)
			}

			url.relativePath = tc.relative

			newURL := FromBytes(url.ToBytes()).(*URL)

			if !reflect.DeepEqual(url, newURL) {
				t.Errorf("got = %q, want %q", url, newURL)
			}
			if !url.deepEqual(newURL) {
				t.Errorf("Not equal")
			}
		})
	}
}
