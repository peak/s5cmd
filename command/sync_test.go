package command

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/google/go-cmp/cmp"
	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/storage"
	"github.com/peak/s5cmd/v2/storage/url"
)

func TestShouldStopSync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		err         error
		exitOnError bool
		want        bool
	}{
		{
			name: "AccessDenied",
			err:  awserr.New("AccessDenied", "access denied", nil),
			want: true,
		},
		{
			name: "NoSuchBucket",
			err:  awserr.New("NoSuchBucket", "no such bucket", nil),
			want: true,
		},
		{
			name: "RequestError",
			err:  awserr.New("RequestError", "request error", nil),
			want: true,
		},
		{
			name: "SerializationError",
			err:  awserr.New("SerializationError", "serialization error", nil),
			want: true,
		},
		{
			name: "SlowDown should not stop",
			err:  awserr.New("SlowDown", "slow down", nil),
			want: false,
		},
		{
			name:        "non-AWS error with exitOnError true",
			err:         fmt.Errorf("some random error"),
			exitOnError: true,
			want:        true,
		},
		{
			name:        "non-AWS error with exitOnError false",
			err:         fmt.Errorf("some random error"),
			exitOnError: false,
			want:        false,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := Sync{
				exitOnError: tc.exitOnError,
			}

			got := s.shouldStopSync(tc.err)
			if got != tc.want {
				t.Errorf("shouldStopSync(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// readPlan runs planRun in the background and returns everything it wrote
// before closing the writer. It fails the test if the writer is never closed.
func readPlan(t *testing.T, ctx context.Context, s Sync, c *cli.Context, onlySource, onlyDest chan *url.URL, common chan *ObjectPair, isBatch bool) []string {
	t.Helper()

	dsturl := mustNewURL(t, "s3://bucket/dst/")
	pr, pw := io.Pipe()

	go s.planRun(ctx, c, onlySource, onlyDest, common, dsturl, NewStrategy(false), pw, isBatch)

	type result struct {
		out []byte
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		out, err := io.ReadAll(pr)
		resultCh <- result{out, err}
	}()

	select {
	case r := <-resultCh:
		if r.err != nil {
			t.Fatalf("unexpected read error: %v", r.err)
		}
		lines := strings.Split(strings.TrimSpace(string(r.out)), "\n")
		if len(lines) == 1 && lines[0] == "" {
			return nil
		}
		sort.Strings(lines)
		return lines
	case <-time.After(10 * time.Second):
		t.Fatal("planRun did not close the writer")
		return nil
	}
}

func TestPlanRunReturnsOnCancelledContext(t *testing.T) {
	t.Parallel()

	for _, del := range []bool{false, true} {
		del := del
		t.Run(fmt.Sprintf("delete=%v", del), func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			// Nothing is ever sent on (or closes) these channels. Without the
			// cancellation checks planRun would block on them forever.
			onlySource := make(chan *url.URL)
			onlyDest := make(chan *url.URL)
			common := make(chan *ObjectPair)

			got := readPlan(t, ctx, Sync{delete: del}, nil, onlySource, onlyDest, common, true)
			if len(got) != 0 {
				t.Errorf("expected no commands, got %v", got)
			}
		})
	}
}

func TestPlanRunGeneratesCommands(t *testing.T) {
	t.Parallel()

	// planRun logs skipped objects through the global logger.
	log.Init("error", false)

	app := cli.NewApp()
	c := cli.NewContext(app, flagSet(t, "sync", nil), nil)

	now := time.Now()
	older := now.Add(-time.Hour)

	onlySource := make(chan *url.URL, 1)
	onlyDest := make(chan *url.URL, 2)
	common := make(chan *ObjectPair, 2)

	onlySource <- mustNewURL(t, "s3://bucket/src/new")
	close(onlySource)

	onlyDest <- mustNewURL(t, "s3://bucket/dst/stale1")
	onlyDest <- mustNewURL(t, "s3://bucket/dst/stale2")
	close(onlyDest)

	// source is newer and differs in size: should be copied.
	common <- &ObjectPair{
		src: &storage.Object{URL: mustNewURL(t, "s3://bucket/src/changed"), Size: 2, ModTime: &now},
		dst: &storage.Object{URL: mustNewURL(t, "s3://bucket/dst/changed"), Size: 1, ModTime: &older},
	}
	// identical: should be skipped.
	common <- &ObjectPair{
		src: &storage.Object{URL: mustNewURL(t, "s3://bucket/src/same"), Size: 1, ModTime: &now},
		dst: &storage.Object{URL: mustNewURL(t, "s3://bucket/dst/same"), Size: 1, ModTime: &now},
	}
	close(common)

	got := readPlan(t, context.Background(), Sync{delete: true}, c, onlySource, onlyDest, common, false)

	want := []string{
		`cp --raw='true' "s3://bucket/src/changed" "s3://bucket/dst/changed"`,
		`cp --raw='true' "s3://bucket/src/new" "s3://bucket/dst/new"`,
		`rm --raw='true' "s3://bucket/dst/stale1" "s3://bucket/dst/stale2"`,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("(-want +got):\n%v", diff)
	}
}
