package e2e

import (
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/igungor/gofakes3"
	"github.com/igungor/gofakes3/backend/s3bolt"
	"github.com/igungor/gofakes3/backend/s3mem"
	"gotest.tools/v3/fs"
)

func s3ServerEndpoint(t *testing.T, testdir *fs.Dir, loglvl, backend string, timeSource gofakes3.TimeSource, enableProxy bool) string {
	var s3backend gofakes3.Backend
	switch backend {
	case "mem":
		s3backend = s3mem.New()
	case "bolt":
		dbpath := testdir.Join("s3.boltdb")
		// we use boltdb as the s3 backend because listing buckets in in-memory
		// backend is not deterministic.
		var err error
		var opts []s3bolt.Option
		if timeSource != nil {
			opts = append(opts, s3bolt.WithTimeSource(timeSource))
		}

		s3backend, err = s3bolt.NewFile(dbpath, opts...)
		if err != nil {
			t.Fatal(err)
		}
	}

	var opts []gofakes3.Option
	withLogger := gofakes3.WithLogger(
		gofakes3.GlobalLog(
			gofakes3.LogLevel(strings.ToUpper(loglvl)),
		),
	)
	opts = append(opts, withLogger)

	if timeSource != nil {
		opts = append(
			opts,
			gofakes3.WithTimeSource(timeSource),
			// disable time skew with custom time source,
			// requests from past or future would cause 'RequestTimeTooSkewed'
			gofakes3.WithTimeSkewLimit(0),
		)
	}
	faker := gofakes3.New(s3backend, opts...)
	s3srv := httptest.NewServer(faker.Server())

	t.Cleanup(func() {
		s3srv.Close()
		// no need to remove boltdb file since 'testdir' will be cleaned up
		// after each test.
	})

	if enableProxy {
		parsedUrl, err := url.Parse(s3srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		proxyEnabledURL := "http://localhost.:" + parsedUrl.Port()
		return proxyEnabledURL
	}
	return s3srv.URL
}
