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
		expected      map[int]compareFunc
		shell         string
	}{
		{
			name:          "cp complete empty string",
			precedingArgs: []string{"cp"},
			arg:           "",
			// local file completions are prepared by the shell
			expected: map[int]compareFunc{},
			shell:    "/bin/bash",
		},
		{
			name:          "cat complete empty string",
			precedingArgs: []string{"cat"},
			arg:           "",
			expected: map[int]compareFunc{
				0: equals("s3://%s/", bucket),
			},
			shell: "/bin/pwsh",
		},
		{
			name:          "mb complete empty string",
			precedingArgs: []string{"mb"},
			arg:           "",
			expected: map[int]compareFunc{
				0: equals("s3://"),
			},
			shell: "/bin/pwsh",
		},
		{
			name:          "mb complete bucket",
			precedingArgs: []string{"mb"},
			arg:           "s3://bu",
			expected: map[int]compareFunc{
				0: equals("s3://bu"),
			},
			shell: "/bin/pwsh",
		},
		{
			name:          "rb complete empty string",
			precedingArgs: []string{"rb"},
			arg:           "",
			expected: map[int]compareFunc{
				0: equals("s3://%s/", bucket),
			},
			shell: "/bin/pwsh",
		},
		{
			name:          "rb should not complete keys string",
			precedingArgs: []string{"rb"},
			arg:           "s3://" + bucket + "/f",
			expected: map[int]compareFunc{
				0: equals("s3://%s/", bucket),
			},
			remoteFiles: []string{
				"file.txt",
				"fdir/child.txt",
			},
			shell: "/bin/pwsh",
		},
		{
			name:          "select complete empty string",
			precedingArgs: []string{"select"},
			arg:           "",
			expected: map[int]compareFunc{
				0: equals("s3://%s/", bucket),
			},
			shell: "/bin/pwsh",
		},
		{
			name:          "cp complete bucket names in pwsh",
			precedingArgs: []string{"cp"},
			arg:           "s3://",
			expected: map[int]compareFunc{
				0: equals("s3://%s/", bucket),
			},
			shell: "/bin/pwsh",
		},
		{
			name:          "cp complete bucket names in zsh",
			precedingArgs: []string{"cp"},
			arg:           "s3://",
			expected: map[int]compareFunc{
				0: equals("s3\\://%s/", bucket),
			},
			shell: "/bin/zsh",
		},
		{
			name:          "cp complete bucket names in bash",
			precedingArgs: []string{"cp"},
			arg:           "s3://",
			expected: map[int]compareFunc{
				0: equals("//%s/", bucket),
				1: equals("s3://%s/", bucket),
			},
			shell: "/bin/bash",
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
			expected: map[int]compareFunc{
				0: equals("s3://%s/dir/", bucket),
				1: equals("s3://%s/file0.txt", bucket),
				2: equals("s3://%s/file1.txt", bucket),
				3: equals("s3://%s/filedir/", bucket),
			},
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
			expected: map[int]compareFunc{
				0: equals("//%s/dir/", bucket),
				1: equals("//%s/file0.txt", bucket),
				2: equals("//%s/file1.txt", bucket),
				3: equals("//%s/filedir/", bucket),
				4: equals("s3://%s/dir/", bucket),
				5: equals("s3://%s/file0.txt", bucket),
				6: equals("s3://%s/file1.txt", bucket),
				7: equals("s3://%s/filedir/", bucket),
			},
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
			expected: map[int]compareFunc{
				0: equals("s3\\://%s/dir/", bucket),
				1: equals("s3\\://%s/file0.txt", bucket),
				2: equals("s3\\://%s/file1.txt", bucket),
				3: equals("s3\\://%s/filedir/", bucket),
			},
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
			expected: map[int]compareFunc{
				0: equals("lon:in:key"),
				1: equals("lonized"),
				2: equals("s3://%s/co:lon:in:key", bucket),
				3: equals("s3://%s/co:lonized", bucket),
			},
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
			expected: map[int]compareFunc{
				0: equals("s3\\://%s/co\\:lon\\:in\\:key", bucket),
				1: equals("s3\\://%s/co\\:lonized", bucket),
			},
			shell: "/bin/zsh",
		},
		{
			name:          "cp complete keys with backslash",
			precedingArgs: []string{"cp"},
			arg:           "s3://" + bucket + "/back\\",
			remoteFiles: []string{
				`back\slash`,
				`backback`,
			},
			expected: map[int]compareFunc{
				0: equals("s3://%s/back\\slash", bucket),
			},
			shell: "/bin/pwsh",
		},
		{
			name:          "cp complete keys with asterisk",
			precedingArgs: []string{"cp"},
			arg:           "s3://" + bucket + "/as*",
			remoteFiles: []string{
				"as*terisk",
				"as*oburiks",
			},
			expected: map[int]compareFunc{
				0: equals("s3://%s/as*oburiks", bucket),
				1: equals("s3://%s/as*terisk", bucket),
			},
			shell: "/bin/pwsh",
		},
		/*
			Question marks and asterisk are thought to be wildcard (special charactes)
			by the s5cmd so when they're given s5cmd's behaviour changes.

			When asterisk is given s5cmd also matches the keys with literal '*' as well as
			all keys that match the URL'S regexp. So the completions with '*' accidentally include the
			keys that contains '*' while the shell scripts filter out those that does not have '*'s.

			On the other hand when the question mark is given then s5cmd do not list keys
			if it is the last character. Because the ? represent a single character and
			it is not expanded to complete remaining of the key.
		*/
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
			res := icmd.RunCmd(cmd, withEnv("SHELL", tc.shell))

			assertLines(t, res.Stdout(), tc.expected, sortInput(true))
		})
	}
}
