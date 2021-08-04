package command

import "testing"

func Test_wildCardToRegexp(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		pattern string
		wanted  string
	}{
		{
			name:    "wildcard to regex : main*",
			pattern: "main*",
			wanted:  "^main.*$",
		},
		{
			name:    "wildcard to regex : *.txt",
			pattern: "*.txt",
			wanted:  "^.*\\.txt$",
		},
		{
			name:    "wildcard to regex : ?_main*.txt",
			pattern: "?_main*.txt",
			wanted:  "^._main.*\\.txt$",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := wildCardToRegexp(tt.pattern); got != tt.wanted {
				t.Errorf("wildCardToRegexp() = %v, want %v", got, tt.wanted)
			}
		})
	}
}
