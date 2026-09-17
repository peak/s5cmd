package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFilesystemImplementsStorageInterface(t *testing.T) {
	var i interface{} = new(Filesystem)
	if _, ok := i.(Storage); !ok {
		t.Errorf("expected %t to implement Storage interface", i)
	}
}

func TestFilesystemCreateDryRun(t *testing.T) {
	t.Parallel()

	fs := &Filesystem{dryRun: true}
	tmpDir := t.TempDir()

	testcases := []struct {
		name   string
		create func() (*os.File, error)
	}{
		{
			name: "Create",
			create: func() (*os.File, error) {
				return fs.Create(filepath.Join(tmpDir, "should-not-exist.txt"))
			},
		},
		{
			name: "CreateTemp",
			create: func() (*os.File, error) {
				return fs.CreateTemp(tmpDir, "dryrun-*.txt")
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := tc.create()
			if err != nil {
				t.Fatalf("%s() returned error: %v", tc.name, err)
			}
			if f == nil {
				t.Fatalf("%s() returned nil file", tc.name)
			}

			if f.Name() != os.DevNull {
				t.Fatalf("expected file name %q, got %q", os.DevNull, f.Name())
			}

			// the handle must not alias stdin, otherwise closing it would
			// close stdin as well.
			if f.Fd() == 0 {
				t.Fatal("expected file descriptor other than stdin")
			}

			if _, err := f.Write([]byte("content")); err != nil {
				t.Fatalf("Write() returned error: %v", err)
			}

			if err := f.Close(); err != nil {
				t.Fatalf("Close() returned error: %v", err)
			}

			if _, err := os.Stdin.Stat(); err != nil {
				t.Fatalf("stdin is not usable after Close(): %v", err)
			}
		})
	}

	// no file should be created on disk during dry run
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no files in temp dir during dry run, got %d", len(entries))
	}
}
