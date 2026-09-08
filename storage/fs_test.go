package storage

import "testing"

func TestFilesystemImplementsStorageInterface(t *testing.T) {
	var i any = new(Filesystem)
	if _, ok := i.(Storage); !ok {
		t.Errorf("expected %t to implement Storage interface", i)
	}
}
