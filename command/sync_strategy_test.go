package command

import (
	"testing"
	"time"

	errorpkg "github.com/peak/s5cmd/v2/error"
	"github.com/peak/s5cmd/v2/storage"
)

func TestSizeAndModificationStrategy_ShouldSync(t *testing.T) {
	ft := time.Now()
	timePtr := func(tt time.Time) *time.Time {
		return &tt
	}
	testcases := []struct {
		name     string
		src      *storage.Object
		dst      *storage.Object
		expected error
	}{

		{
			//	time: src > dst       size: src != dst
			name:     "source is newer, sizes are different",
			src:      &storage.Object{ModTime: timePtr(ft.Add(time.Minute)), Size: 10},
			dst:      &storage.Object{ModTime: timePtr(ft), Size: 5},
			expected: nil,
		},

		{
			//	time: src > dst       size: src = dst
			name:     "source is newer, sizes are same",
			src:      &storage.Object{ModTime: timePtr(ft.Add(time.Minute)), Size: 10},
			dst:      &storage.Object{ModTime: timePtr(ft), Size: 10},
			expected: nil,
		},

		{
			//	time: src < dst       size: src != dst
			name:     "source is older, sizes are different",
			src:      &storage.Object{ModTime: timePtr(ft), Size: 10},
			dst:      &storage.Object{ModTime: timePtr(ft.Add(time.Minute)), Size: 5},
			expected: nil,
		},

		{
			//	time: src < dst       size: src == dst
			name:     "source is older, sizes are same",
			src:      &storage.Object{ModTime: timePtr(ft), Size: 10},
			dst:      &storage.Object{ModTime: timePtr(ft.Add(time.Minute)), Size: 10},
			expected: errorpkg.ErrObjectIsNewerAndSizesMatch,
		},

		{
			//	time: src = dst       size: src != dst
			name:     "files have same age, sizes are different",
			src:      &storage.Object{ModTime: timePtr(ft), Size: 10},
			dst:      &storage.Object{ModTime: timePtr(ft), Size: 5},
			expected: nil,
		},

		{
			//	time: src = dst       size: src == dst
			name:     "files have same age, sizes are same",
			src:      &storage.Object{ModTime: timePtr(ft), Size: 10},
			dst:      &storage.Object{ModTime: timePtr(ft), Size: 10},
			expected: errorpkg.ErrObjectIsNewerAndSizesMatch,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			strategy := &SizeAndModificationStrategy{}
			if got := strategy.ShouldSync(tc.src, tc.dst); got != tc.expected {
				t.Fatalf("expected: %q(%T), got: %q(%T)", tc.expected, tc.expected, got, got)
			}
		})
	}
}

func TestSizeOnlyStrategy_ShouldSync(t *testing.T) {
	ft := time.Now()
	timePtr := func(tt time.Time) *time.Time {
		return &tt
	}
	testcases := []struct {
		name     string
		src      *storage.Object
		dst      *storage.Object
		expected error
	}{

		{
			//	time: src > dst       size: src != dst
			name:     "source is newer, sizes are different",
			src:      &storage.Object{ModTime: timePtr(ft.Add(time.Minute)), Size: 10},
			dst:      &storage.Object{ModTime: timePtr(ft), Size: 5},
			expected: nil,
		},

		{
			//	time: src > dst       size: src = dst
			name:     "source is newer, sizes are same",
			src:      &storage.Object{ModTime: timePtr(ft.Add(time.Minute)), Size: 10},
			dst:      &storage.Object{ModTime: timePtr(ft), Size: 10},
			expected: errorpkg.ErrObjectSizesMatch,
		},

		{
			//	time: src < dst       size: src != dst
			name:     "source is older, sizes are different",
			src:      &storage.Object{ModTime: timePtr(ft), Size: 10},
			dst:      &storage.Object{ModTime: timePtr(ft.Add(time.Minute)), Size: 5},
			expected: nil,
		},

		{
			//	time: src < dst       size: src == dst
			name:     "source is older, sizes are same",
			src:      &storage.Object{ModTime: timePtr(ft), Size: 10},
			dst:      &storage.Object{ModTime: timePtr(ft.Add(time.Minute)), Size: 10},
			expected: errorpkg.ErrObjectSizesMatch,
		},

		{
			//	time: src = dst       size: src != dst
			name:     "files have same age, sizes are different",
			src:      &storage.Object{ModTime: timePtr(ft), Size: 10},
			dst:      &storage.Object{ModTime: timePtr(ft), Size: 5},
			expected: nil,
		},

		{
			//	time: src = dst       size: src == dst
			name:     "files have same age, sizes are same",
			src:      &storage.Object{ModTime: timePtr(ft), Size: 10},
			dst:      &storage.Object{ModTime: timePtr(ft), Size: 10},
			expected: errorpkg.ErrObjectSizesMatch,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			strategy := &SizeOnlyStrategy{}
			if got := strategy.ShouldSync(tc.src, tc.dst); got != tc.expected {
				t.Fatalf("expected: %q(%T), got: %q(%T)", tc.expected, tc.expected, got, got)
			}
		})
	}
}
