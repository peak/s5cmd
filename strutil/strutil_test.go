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
		b    int64
		want string
	}{
		{0, "0B"},
		{1, "1B"},
		{999, "999B"},
		{1023, "1023B"},
		{1024, "1024B"},
		{1025, "1.0K"},
		{1536, "1.5K"},
		{1 << 20, "1024.0K"},
		{(1 << 20) + 1, "1.0M"},
		{5 * (1 << 30), "5.0G"},
		{3 * (1 << 40), "3.0T"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := HumanizeBytes(tt.b); got != tt.want {
				t.Errorf("HumanizeBytes(%d) = %q, want %q", tt.b, got, tt.want)
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
