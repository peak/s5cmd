package strutil

import "testing"

func TestCapitalizeFirstLetter(t *testing.T) {
	tests := []struct {
		name string
		arg  string
		want string
	}{
		{
			name: "empty string",
			arg:  "",
			want: "",
		},
		{
			name: "single rune",
			arg:  "s",
			want: "S",
		},
		{
			name: "normal word",
			arg:  "sUsPend",
			want: "Suspend",
		},
		{
			name: "with number",
			arg:  "numb3r",
			want: "Numb3r",
		},
		{
			name: "two words",
			arg:  "two words",
			want: "Two words",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CapitalizeFirstRune(tt.arg); got != tt.want {
				t.Errorf("CapitalizeFirstRune() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHumanizeBytes(t *testing.T) {
	tests := []struct {
		name string
		arg  int64
		want string
	}{
		{
			name: "zero bytes",
			arg:  0,
			want: "0B",
		},
		{
			name: "500 bytes",
			arg:  500,
			want: "500B",
		},
		{
			name: "1025 bytes to kilobytes",
			arg:  1025,
			want: "1.0K",
		},
		{
			name: "exactly 1 megabyte stays in kilobytes",
			arg:  1048576,
			want: "1024.0K",
		},
		{
			name: "over 1 megabyte",
			arg:  1048577,
			want: "1.0M",
		},
		{
			name: "5 gigabytes",
			arg:  5368709121,
			want: "5.0G",
		},
		{
			name: "over 1 terabyte",
			arg:  1099511627777,
			want: "1.0T",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HumanizeBytes(tt.arg); got != tt.want {
				t.Errorf("HumanizeBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_WildCardToRegexp(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		pattern string
		wanted  string
	}{
		{
			name:    "main*",
			pattern: "main*",
			wanted:  "main.*",
		},
		{
			name:    "*.txt",
			pattern: "*.txt",
			wanted:  ".*\\.txt",
		},
		{
			name:    "?_main*.txt",
			pattern: "?_main*.txt",
			wanted:  "._main.*\\.txt",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WildCardToRegexp(tt.pattern); got != tt.wanted {
				t.Errorf("wildCardToRegexp() = %v, want %v", got, tt.wanted)
			}
		})
	}
}
