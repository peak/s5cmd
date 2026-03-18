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
	path := filepath.Join(tmpDir, "should-not-exist.txt")

	// Create should return a non-nil file and no error
	f, err := fs.Create(path)
	if err != nil {
		t.Fatalf("Create() returned error: %v", err)
	}
	if f == nil {
		t.Fatal("Create() returned nil file")
	}

	// The returned file should reference os.DevNull
	if f.Name() != os.DevNull {
		t.Fatalf("expected file name %q, got %q", os.DevNull, f.Name())
	}

	// Verify no actual file was created on disk
	_, err = os.Stat(path)
	if err == nil {
		t.Fatal("expected file to not exist on disk during dry run, but it does")
	}
	if !os.IsNotExist(err) {
		t.Fatalf("expected IsNotExist error, got: %v", err)
	}

	// Test CreateTemp with dryRun
	tf, err := fs.CreateTemp(tmpDir, "dryrun-*.txt")
	if err != nil {
		t.Fatalf("CreateTemp() returned error: %v", err)
	}
	if tf == nil {
		t.Fatal("CreateTemp() returned nil file")
	}

	// The returned file should reference os.DevNull
	if tf.Name() != os.DevNull {
		t.Fatalf("expected temp file name %q, got %q", os.DevNull, tf.Name())
	}

	// Verify no temp file was created in the directory
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no files in temp dir during dry run, got %d", len(entries))
	}
}
