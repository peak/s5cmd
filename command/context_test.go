package command

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/kballard/go-shellquote"
	"github.com/urfave/cli/v3"

	"github.com/peak/s5cmd/v2/storage/url"
)

func TestGenerateCommand(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name            string
		cmd             string
		flags           []cli.Flag
		defaultFlags    map[string]any
		urls            []*url.URL
		expectedCommand string
	}{
		{
			name:  "empty-cli-flags",
			cmd:   "cp",
			flags: []cli.Flag{},
			urls: []*url.URL{
				mustNewURL(t, "s3://bucket/key1"),
				mustNewURL(t, "s3://bucket/key2"),
			},
			expectedCommand: `cp s3://bucket/key1 s3://bucket/key2`,
		},
		{
			name:  "empty-cli-flags-with-default-flags",
			cmd:   "cp",
			flags: []cli.Flag{},
			defaultFlags: map[string]any{
				"raw": true,
				"acl": "public-read",
			},
			urls: []*url.URL{
				mustNewURL(t, "s3://bucket/key1"),
				mustNewURL(t, "s3://bucket/key2"),
			},
			expectedCommand: `cp --acl='public-read' --raw='true' s3://bucket/key1 s3://bucket/key2`,
		},
		{
			name: "cli-flag-with-whitespaced-flag-value",
			cmd:  "cp",
			flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "cache-control",
					Value: "public, max-age=31536000, immutable",
				},
			},
			defaultFlags: map[string]any{
				"raw": true,
			},
			urls: []*url.URL{
				mustNewURL(t, "s3://bucket/key1"),
				mustNewURL(t, "s3://bucket/key2"),
			},
			expectedCommand: `cp --cache-control='public, max-age=31536000, immutable' --raw='true' s3://bucket/key1 s3://bucket/key2`,
		},
		{
			name: "same-flag-should-be-ignored-if-given-from-both-default-and-cli-flags",
			cmd:  "cp",
			flags: []cli.Flag{
				&cli.BoolFlag{
					Name:  "raw",
					Value: true,
				},
			},
			defaultFlags: map[string]any{
				"raw": true,
			},
			urls: []*url.URL{
				mustNewURL(t, "s3://bucket/key1"),
				mustNewURL(t, "s3://bucket/key2"),
			},
			expectedCommand: `cp --raw='true' s3://bucket/key1 s3://bucket/key2`,
		},
		{
			name: "ignore-non-shared-flag",
			cmd:  "cp",
			flags: []cli.Flag{
				&cli.BoolFlag{
					Name:  "force-glacier-transfer",
					Value: true,
				},
				&cli.BoolFlag{
					Name:  "raw",
					Value: true,
				},
				&cli.BoolFlag{
					Name:  "flatten",
					Value: true,
				},
				&cli.IntFlag{
					Name:  "concurrency",
					Value: 6,
				},
				// delete is not shared flag, will be ignored
				&cli.BoolFlag{
					Name:  "delete",
					Value: true,
				},
				// size-only is not shared flag, will be ignored
				&cli.BoolFlag{
					Name:  "size-only",
					Value: true,
				},
			},
			urls: []*url.URL{
				mustNewURL(t, "s3://bucket/key1"),
				mustNewURL(t, "s3://bucket/key2"),
			},
			expectedCommand: `cp --concurrency='6' --flatten='true' --force-glacier-transfer='true' --raw='true' s3://bucket/key1 s3://bucket/key2`,
		},
		{
			name: "string-slice-flag",
			cmd:  "cp",
			flags: []cli.Flag{
				&cli.StringSliceFlag{
					Name:  "exclude",
					Value: []string{"*.txt", "*.log"},
				},
			},
			urls: []*url.URL{
				mustNewURL(t, "/source/dir"),
				mustNewURL(t, "s3://bucket/prefix/"),
			},
			expectedCommand: `cp --exclude='*.log' --exclude='*.txt' /source/dir s3://bucket/prefix/`,
		},
		{
			name:  "command-with-multiple-args",
			cmd:   "rm",
			flags: []cli.Flag{},
			urls: []*url.URL{
				mustNewURL(t, "s3://bucket/key1"),
				mustNewURL(t, "s3://bucket/key2"),
				mustNewURL(t, "s3://bucket/prefix/key3"),
				mustNewURL(t, "s3://bucket/prefix/key4"),
			},
			expectedCommand: `rm s3://bucket/key1 s3://bucket/key2 s3://bucket/prefix/key3 s3://bucket/prefix/key4`,
		},
		{
			name:  "command-args-with-spaces",
			cmd:   "rm",
			flags: []cli.Flag{},
			urls: []*url.URL{
				mustNewURL(t, "file with space"),
				mustNewURL(t, "wow wow"),
			},
			expectedCommand: `rm 'file with space' 'wow wow'`,
		},
	}
	for _, tc := range testcases {

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			command := AppCommand(tc.cmd)

			// v3 has no cli.Context to synthesise, so run a stand-in
			// command with the same flags and let cli parse them: that
			// is what marks a flag "set", which generateCommand keys on.
			var got string
			runner := &cli.Command{
				Name:  command.Name,
				Flags: tc.flags,
				Action: func(_ context.Context, c *cli.Command) error {
					got = generateCommand(c, command.Name, tc.defaultFlags, tc.urls...)
					return nil
				},
			}

			args := append([]string{command.Name}, flagArgs(t, tc.flags)...)
			if err := runner.Run(context.Background(), args); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.expectedCommand, got); diff != "" {
				t.Errorf("(-want +got):\n%v", diff)
			}
		})
	}
}

// TestGenerateCommandKeyRoundTrip asserts that object keys survive the
// round-trip from a generated command back through the parser run-mode uses
// to read it. Keys containing a literal tab used to be rendered by %q as the
// two characters \ and t, which a shell parser does not treat as an escape,
// so sync produced a copy command for a key that did not exist.
func TestGenerateCommandKeyRoundTrip(t *testing.T) {
	t.Parallel()

	keys := []string{
		"tab\tseparated.txt",
		"new\nline.txt",
		"carriage\rreturn.txt",
		"vertical\ttab\tand space.txt",
		"back\\slash.txt",
		`double"quote.txt`,
		"single'quote.txt",
		"dollar$and`backtick.txt",
		"plain space.txt",
		"ordinary.txt",
	}

	command := AppCommand("cp")

	for _, key := range keys {
		t.Run(strconv.Quote(key), func(t *testing.T) {
			t.Parallel()

			src := mustNewURL(t, "s3://bucket/"+key)
			dst := mustNewURL(t, "s3://other-bucket/"+key)

			var got string
			runner := &cli.Command{
				Name: command.Name,
				Action: func(_ context.Context, c *cli.Command) error {
					got = generateCommand(c, command.Name, map[string]any{}, src, dst)
					return nil
				},
			}
			if runErr := runner.Run(context.Background(), []string{command.Name}); runErr != nil {
				t.Fatal(runErr)
			}

			fields, err := shellquote.Split(got)
			if err != nil {
				t.Fatalf("generated command does not parse: %v\ncommand: %s", err, got)
			}

			want := []string{"cp", src.String(), dst.String()}
			if diff := cmp.Diff(want, fields); diff != "" {
				t.Errorf("round-trip of %s lost the key (-want +got):\n%v", strconv.Quote(key), diff)
			}
		})
	}
}

func mustNewURL(t *testing.T, path string) *url.URL {
	t.Helper()

	u, err := url.New(path)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// flagArgs renders flags as the command-line arguments that would set them
// to their declared values.
func flagArgs(t *testing.T, flags []cli.Flag) []string {
	t.Helper()

	var args []string
	for _, f := range flags {
		name := f.Names()[0]
		switch v := f.(type) {
		case *cli.StringSliceFlag:
			for _, s := range v.Value {
				args = append(args, fmt.Sprintf("--%s=%s", name, s))
			}
		case *cli.BoolFlag:
			if v.Value {
				args = append(args, "--"+name)
			}
		case *cli.IntFlag:
			args = append(args, fmt.Sprintf("--%s=%d", name, v.Value))
		case *cli.StringFlag:
			args = append(args, fmt.Sprintf("--%s=%s", name, v.Value))
		default:
			t.Fatalf("flagArgs: unhandled flag type %T", f)
		}
	}
	return args
}
