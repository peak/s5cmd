package command

/*
func TestSyncGenerateCommand(t *testing.T) {
	t.Parallel()

	app := cli.NewApp()

	type flagset struct {
		name  string
		value interface{}
	}

	testcases := []struct {
		name            string
		cmd             string
		flags           []flagset
		ctx             *cli.Context
		urls            []*url.URL
		expectedCommand string
	}{
		{
			cmd: "cp",
			flags: []flagset{
				{name: "force-glacier-transfer", value: true},
				{name: "raw", value: true},
				{name: "delete", value: true},
				{name: "flatten", value: true},
			},
			urls: []*url.URL{
				MustNewURL(t, "s3://bucket/key"),
				MustNewURL(t, "s3://bucket/key2"),
			},
			expectedCommand: "cp --raw=true s3://bucket/key s3://bucket/key2",
		},
	}
	for _, tc := range testcases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			command := AppCommand("sync")
			set, err := flagSet(tc.cmd, command.Flags)
			if err != nil {
				t.Fatal(err)
			}

			ctx := cli.NewContext(app, set, ctx)
			ctx.Command = command

			cmd := Sync{}.GenerateCommand(ctx, command.Name)

			assert.Equal(t, cmd, tc.expectedCommand)

			// for _, flg := range tc.flags {}
			// sync := Sync{}
			// cli.NewContext(app, tc.flags, nil)
			// cmd := sync.GenerateCommand(app, tc.cmd, tc.flags)

		})
	}
}

func MustNewURL(t *testing.T, path string) *url.URL {
	t.Helper()

	u, err := url.New(path)
	if err != nil {
		t.Fatal(err)
	}

	return u
}

func flagSet(name string, flags []cli.Flag) (*flag.FlagSet, error) {
	set := flag.NewFlagSet(name, flag.ContinueOnError)

	for _, f := range flags {
		if err := f.Apply(set); err != nil {
			return nil, err
		}
	}
	set.SetOutput(ioutil.Discard)
	return set, nil
}
*/
