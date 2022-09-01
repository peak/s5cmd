package e2e

import (
	"testing"

	"gotest.tools/v3/icmd"
)

func TestCompletionFlag(t *testing.T) {
	flag := "--generate-bash-completion"
	bucket := s3BucketFromTestName(t)

	testcases := []struct {
		name          string
		precedingArgs []string
		arg           string
		remoteFiles   []string
		expected      []string
		shell         string
	}{
		{
			name:          "cp complete empty string",
			precedingArgs: []string{"cp"},
			arg:           "",
			// local file completions are prepared by the shell
			expected: []string{},
			shell:    "/bin/bash",
		},
		{
			name:          "cp complete bucket names in pwsh",
			precedingArgs: []string{"cp"},
			arg:           "s3://",
			expected:      []string{"s3://" + bucket + "/"},
			shell:         "/bin/pwsh",
		},
		{
			name:          "cp complete bucket names in zsh",
			precedingArgs: []string{"cp"},
			arg:           "s3://",
			expected:      []string{"s3\\://" + bucket + "/"},
			shell:         "/bin/zsh",
		},
		{
			name:          "cp complete bucket names in bash",
			precedingArgs: []string{"cp"},
			arg:           "s3://",
			expected:      []string{"s3://" + bucket + "/", "//" + bucket + "/"},
			shell:         "/bin/bash",
		},
		{
			name:          "cp complete bucket keys pwsh",
			precedingArgs: []string{"cp"},
			arg:           "s3://" + bucket + "/",
			remoteFiles: []string{
				"file0.txt",
				"file1.txt",
				"filedir/child.txt",
				"dir/child.txt",
			},
			expected: keysToS3URL("s3://", bucket,
				"file0.txt",
				"file1.txt",
				"filedir/",
				"dir/",
			),
			shell: "/bin/pwsh",
		},
		{
			name:          "cp complete bucket keys bash",
			precedingArgs: []string{"cp"},
			arg:           "s3://" + bucket + "/",
			remoteFiles: []string{
				"file0.txt",
				"file1.txt",
				"filedir/child.txt",
				"dir/child.txt",
			},
			expected: append(
				keysToS3URL("s3://", bucket,
					"file0.txt",
					"file1.txt",
					"filedir/",
					"dir/"),
				keysToS3URL("//", bucket,
					"file0.txt",
					"file1.txt",
					"filedir/",
					"dir/")...),
			shell: "/bin/bash",
		},
		{
			name:          "cp complete bucket keys zsh",
			precedingArgs: []string{"cp"},
			arg:           "s3://" + bucket + "/",
			remoteFiles: []string{
				"file0.txt",
				"file1.txt",
				"filedir/child.txt",
				"dir/child.txt",
			},
			expected: keysToS3URL("s3\\://", bucket,
				"file0.txt",
				"file1.txt",
				"filedir/",
				"dir/",
			),
			shell: "/bin/zsh",
		},
		{
			name:          "cp complete keys with colon bash",
			precedingArgs: []string{"cp"},
			arg:           "s3://" + bucket + "/co:lo",
			remoteFiles: []string{
				"co:lon:in:key",
				"co:lonized",
			},
			expected: append(
				keysToS3URL("s3://", bucket, "co:lon:in:key", "co:lonized"),
				"lon:in:key", "lonized"),
			shell: "/bin/bash",
		},
		{
			name:          "cp complete keys with colon zsh",
			precedingArgs: []string{"cp"},
			arg:           "s3://" + bucket + "/co:lo",
			remoteFiles: []string{
				"co:lon:in:key",
				"co:lonized",
			},
			expected: keysToS3URL("s3\\://", bucket, "co\\:lon\\:in\\:key", "co\\:lonized"),
			shell:    "/bin/zsh",
		},
		{
			name:          "cp complete keys with asterisk",
			precedingArgs: []string{"cp"},
			arg:           "s3://" + bucket + "/as*",
			remoteFiles: []string{
				"as*terisk",
				"as*oburiks",
			},
			expected: keysToS3URL("s3://", bucket, "as*terisk", "as*oburiks"),
			shell:    "/bin/pwsh",
		},
		/* Question marks are thought to be wildcard by the s5cmd so they cannot be properly handled yet
		{
			name:          "cp complete keys with question mark",
			precedingArgs: []string{"cp", "--raw"},
			arg:           "s3://" + bucket + "/qu?",
			remoteFiles: []string{
				"qu?estion",
				"qu?vestion",
			},
			expected: keysToS3URL("s3://", bucket,
				"qu?estion", "qu?vestion"),
			shell: "/bin/pwsh",
		},
		*/
		{
			name:          "cp complete keys with backslash",
			precedingArgs: []string{"cp"},
			arg:           "s3://" + bucket + "/back\\",
			remoteFiles: []string{
				`back\slash`,
				`backback`,
			},
			expected: keysToS3URL("s3://", bucket,
				`back\slash`),
			shell: "/bin/pwsh",
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s3client, s5cmd := setup(t)

			// prepare remote bucket content
			createBucket(t, s3client, bucket)

			for _, f := range tc.remoteFiles {
				putFile(t, s3client, bucket, f, "content")
			}

			cmd := s5cmd(append(tc.precedingArgs, tc.arg, flag)...)
			result := icmd.RunCmd(cmd, withEnv("SHELL", tc.shell))

			assertLines(t, result.Stdout(), expectedSliceToEqualsMap(tc.expected, true), sortInput(true))
		})
	}
}
