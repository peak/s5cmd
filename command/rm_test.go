package command

import (
	"flag"
	"testing"

	"github.com/urfave/cli/v2"
)

// TestRemoveCommand_DirobjKeyWithRawFlag verifies that a key ending with "/"
// (a directory-marker object, "DIROBJ") can be targeted when --raw is set.
// Without --raw the prefix guard correctly rejects such keys; with --raw the
// user has explicitly opted out of prefix treatment and the key must pass
// validation so that the actual S3 DeleteObject call can proceed.
func TestRemoveCommand_DirobjKeyWithRawFlag(t *testing.T) {
	t.Parallel()

	// Build a flagset that mirrors what NewDeleteCommand registers.
	cmd := NewDeleteCommand()
	set := flagSet(t, cmd.Name, cmd.Flags)

	// Simulate: s5cmd rm --raw s3://bucket/dirobj/
	if err := set.Set("raw", "true"); err != nil {
		t.Fatalf("setting --raw flag: %v", err)
	}
	if err := set.Parse([]string{"s3://bucket/dirobj/"}); err != nil {
		t.Fatalf("parsing args: %v", err)
	}

	ctx := cli.NewContext(nil, set, nil)
	if err := validateRMCommand(ctx); err != nil {
		t.Errorf("expected no error with --raw for a key ending in '/'; got: %v", err)
	}
}

// TestRemoveCommand_PrefixWithoutRawFlag verifies that the prefix guard still
// rejects keys ending with "/" when --raw is not set.
func TestRemoveCommand_PrefixWithoutRawFlag(t *testing.T) {
	t.Parallel()

	cmd := NewDeleteCommand()
	set := flagSet(t, cmd.Name, cmd.Flags)

	if err := set.Parse([]string{"s3://bucket/prefix/"}); err != nil {
		t.Fatalf("parsing args: %v", err)
	}

	ctx := cli.NewContext(nil, set, nil)
	err := validateRMCommand(ctx)
	if err == nil {
		t.Error("expected error for prefix key without --raw; got nil")
	}
}

func TestValidateRMCommand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		sources        []string
		expectedErrStr string
	}{
		{
			name: "error_if_local_and_remote_mixed",
			sources: []string{
				"s3://bucket/key",
				"filename.txt",
			},
			expectedErrStr: "arguments cannot have both local and remote sources",
		},
		{
			name: "error_if_sources_have_bucket",
			sources: []string{
				"s3://bucket/key",
				"s3://bucket",
			},
			expectedErrStr: "s3 bucket/prefix cannot be used for delete operations (forgot wildcard character?)",
		},
		{
			name: "error_if_sources_have_s3_prefix",
			sources: []string{
				"s3://bucket/prefix/",
			},
			expectedErrStr: "s3 bucket/prefix cannot be used for delete operations (forgot wildcard character?)",
		},
		{
			name: "success",
			sources: []string{
				"s3://bucket/prefix/filename.txt",
				"s3://bucket/wildcard/*.txt",
			},
		},
		{
			name: "error_if_different_buckets",
			sources: []string{
				"s3://bucket/object",
				"s3://someotherbucket/object",
			},
			expectedErrStr: "removal of objects with different buckets in a single command is not allowed",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			flagset := flag.NewFlagSet("rm", flag.ExitOnError)
			if err := flagset.Parse(tc.sources); err != nil {
				t.Error(err)
			}
			ctx := cli.NewContext(app, flagset, nil)

			err := validateRMCommand(ctx)
			if (err != nil && err.Error() != tc.expectedErrStr) ||
				(err == nil && tc.expectedErrStr != "") {
				t.Errorf("expected_got = %v, error_expected = %v", err, tc.expectedErrStr)
			}
		})
	}
}
