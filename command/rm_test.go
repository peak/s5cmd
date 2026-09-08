package command

import (
	"context"
	"testing"

	"github.com/urfave/cli/v3"
)

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
		t.Run(tc.name, func(t *testing.T) {
			// v3 has no cli.Context; run a stand-in command so the
			// arguments are parsed the way they would be in practice.
			var err error
			cmd := &cli.Command{
				Name: "rm",
				Action: func(_ context.Context, c *cli.Command) error {
					err = validateRMCommand(c)
					return nil
				},
			}
			if runErr := cmd.Run(context.Background(), append([]string{"rm"}, tc.sources...)); runErr != nil {
				t.Fatal(runErr)
			}

			if (err != nil && err.Error() != tc.expectedErrStr) ||
				(err == nil && tc.expectedErrStr != "") {
				t.Errorf("expected_got = %v, error_expected = %v", err, tc.expectedErrStr)
			}
		})
	}
}
