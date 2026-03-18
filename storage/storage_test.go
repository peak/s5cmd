package storage

import (
	"testing"
	"time"

	"github.com/peak/s5cmd/v2/storage/url"
)

func TestObjectSerializeRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Nanosecond)
	u, err := url.New("s3://bucket/path/to/object.txt")
	if err != nil {
		t.Fatal(err)
	}

	original := Object{
		URL:     u,
		ModTime: &now,
		Type:    ObjectType{mode: 0},
		Size:    12345,
	}

	data, err := original.ToBytes()
	if err != nil {
		t.Fatalf("ToBytes failed: %v", err)
	}

	restored, err := FromBytes(data)
	if err != nil {
		t.Fatalf("FromBytes failed: %v", err)
	}

	if restored.URL.Absolute() != original.URL.Absolute() {
		t.Errorf("URL mismatch: got %q, want %q", restored.URL.Absolute(), original.URL.Absolute())
	}
	if !restored.ModTime.Equal(*original.ModTime) {
		t.Errorf("ModTime mismatch: got %v, want %v", restored.ModTime, original.ModTime)
	}
	if restored.Size != original.Size {
		t.Errorf("Size mismatch: got %d, want %d", restored.Size, original.Size)
	}
	if restored.Type.mode != original.Type.mode {
		t.Errorf("Type mismatch: got %v, want %v", restored.Type.mode, original.Type.mode)
	}
}

func TestObjectCompare(t *testing.T) {
	mkObj := func(path string) Object {
		u, _ := url.New(path)
		return Object{URL: u}
	}

	tests := []struct {
		a, b     string
		expected int
	}{
		{"s3://bucket/a", "s3://bucket/b", -1},
		{"s3://bucket/b", "s3://bucket/a", 1},
		{"s3://bucket/same", "s3://bucket/same", 0},
		{"s3://bucket/abc", "s3://bucket/xyz", -1},
	}

	for _, tc := range tests {
		got := Compare(mkObj(tc.a), mkObj(tc.b))
		if (tc.expected < 0 && got >= 0) || (tc.expected > 0 && got <= 0) || (tc.expected == 0 && got != 0) {
			t.Errorf("Compare(%q, %q) = %d, want sign %d", tc.a, tc.b, got, tc.expected)
		}
	}
}
