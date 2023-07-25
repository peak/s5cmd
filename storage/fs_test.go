package storage

import (
	"os"
	"syscall"
	"testing"
)

func TestFilesystemImplementsStorageInterface(t *testing.T) {
	var i interface{} = new(Filesystem)
	if _, ok := i.(Storage); !ok {
		t.Errorf("expected %t to implement Storage interface", i)
	}
}

func TestIsSpecialFile(t *testing.T) {
	fs := Filesystem{}

	testcases := []struct {
		name string
		mode uint32
	}{
		{
			name: "character_file",
			mode: uint32(os.ModeCharDevice),
		},
		{
			name: "block_file",
			mode: uint32(os.ModeDevice),
		},
		{
			name: "fifo",
			mode: uint32(os.ModeNamedPipe),
		},
		{
			name: "socket",
			mode: uint32(os.ModeSocket),
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := syscall.Mknod(tc.name, tc.mode, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(tc.name)

			isSpecial, err := fs.IsSpecialFile(tc.name)
			if err != nil {
				t.Fatalf("Error checking file: %v", err)
			}
			fileInfo, _ := os.Stat(tc.name)

			mode := fileInfo.Mode()
			if !isSpecial {
				t.Errorf("Expected character special file to be recognized as a special file, but it is not. %v", mode)
			}
		})
	}

}
