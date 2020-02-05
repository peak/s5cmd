package e2e

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3bolt"
	"gotest.tools/v3/fs"
)

func s3ServerEndpoint(t *testing.T, testdir *fs.Dir, loglvl string) (string, func()) {
	dbpath := testdir.Join("s3.boltdb")
	// we use boltdb as the s3 backend because listing buckets in in-memory
	// backend is not deterministic.
	s3backend, err := s3bolt.NewFile(dbpath)
	if err != nil {
		t.Fatal(err)
	}

	withLogger := gofakes3.WithLogger(
		gofakes3.GlobalLog(
			gofakes3.LogLevel(strings.ToUpper(loglvl)),
		),
	)
	faker := gofakes3.New(s3backend, withLogger)
	s3srv := httptest.NewServer(faker.Server())

	cleanup := func() {
		s3srv.Close()
		// no need to remove boltdb file since 'testdir' will be cleaned up
		// after each test.
	}

	return s3srv.URL, cleanup
}
