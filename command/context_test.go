package command

import (
	"flag"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/peak/s5cmd/storage/url"
	"github.com/urfave/cli/v2"
)

func TestGenerateCommand(t *testing.T) {
	t.Parallel()

	app := cli.NewApp()

	testcases := []struct {
		name            string
		cmd             string
		flags           []cli.Flag
		defaultFlags    map[string]interface{}
		ctx             *cli.Context
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
			expectedCommand: `cp "s3://bucket/key1" "s3://bucket/key2"`,
		},
		{
			name:  "empty-cli-flags-with-default-flags",
			cmd:   "cp",
			flags: []cli.Flag{},
			defaultFlags: map[string]interface{}{
				"raw": true,
				"acl": "public-read",
			},
			urls: []*url.URL{
				mustNewURL(t, "s3://bucket/key1"),
				mustNewURL(t, "s3://bucket/key2"),
			},
			expectedCommand: `cp --acl=public-read --raw=true "s3://bucket/key1" "s3://bucket/key2"`,
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
			defaultFlags: map[string]interface{}{
				"raw": true,
			},
			urls: []*url.URL{
				mustNewURL(t, "s3://bucket/key1"),
				mustNewURL(t, "s3://bucket/key2"),
			},
			expectedCommand: `cp --raw=true "s3://bucket/key1" "s3://bucket/key2"`,
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
			expectedCommand: `cp --concurrency=6 --flatten=true --force-glacier-transfer=true --raw=true "s3://bucket/key1" "s3://bucket/key2"`,
		},
		{
			name: "string-slice-flag",
			cmd:  "cp",
			flags: []cli.Flag{
				&cli.StringSliceFlag{
					Name:  "exclude",
					Value: cli.NewStringSlice("*.txt", "*.log"),
				},
			},
			urls: []*url.URL{
				mustNewURL(t, "/source/dir"),
				mustNewURL(t, "s3://bucket/prefix/"),
			},
			expectedCommand: `cp --exclude=*.log --exclude=*.txt "/source/dir" "s3://bucket/prefix/"`,
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
			expectedCommand: `rm "s3://bucket/key1" "s3://bucket/key2" "s3://bucket/prefix/key3" "s3://bucket/prefix/key4"`,
		},
		{
			name:  "command-args-with-spaces",
			cmd:   "rm",
			flags: []cli.Flag{},
			urls: []*url.URL{
				mustNewURL(t, "file with space"),
				mustNewURL(t, "wow wow"),
			},
			expectedCommand: `rm "file with space" "wow wow"`,
		},
	}
	for _, tc := range testcases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			command := AppCommand(tc.cmd)
			set := flagSet(t, command.Name, tc.flags)
			ctx := cli.NewContext(app, set, nil)

			// urfave.Cli pass flags values to context before calling command.Action()
			// and methods to update context are package-private, so write simple
			// flag parser to update context value.
			set.VisitAll(func(f *flag.Flag) {
				value := strings.Trim(f.Value.String(), "[")
				value = strings.Trim(value, "]")
				for _, v := range strings.Fields(value) {
					ctx.Set(f.Name, v)
				}
			})

			got, err := generateCommand(ctx, command.Name, tc.defaultFlags, tc.urls...)
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(tc.expectedCommand, got); diff != "" {
				t.Errorf("(-want +got):\n%v", diff)
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

func flagSet(t *testing.T, name string, flags []cli.Flag) *flag.FlagSet {
	t.Helper()

	set := flag.NewFlagSet(name, flag.ContinueOnError)
	for _, f := range flags {
		if err := f.Apply(set); err != nil {
			t.Fatal(err)
		}
	}
	return set
}
