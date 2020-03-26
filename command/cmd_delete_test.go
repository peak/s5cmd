package command

import (
	"testing"
)

func TestSourcesHaveSameType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sources []string
		wantErr bool
	}{
		{
			name: "error_if_local_and_remote_mixed",
			sources: []string{
				"s3://bucket/key",
				"filename.txt",
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
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if err := sourcesHaveSameType(tc.sources...); (err != nil) != tc.wantErr {
				t.Errorf("checkSources() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
