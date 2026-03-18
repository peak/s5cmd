package command

import (
	"flag"
	"strings"
	"testing"

	"github.com/urfave/cli/v2"
)

func TestCheckNumberOfArguments(t *testing.T) {
	tests := []struct {
		name      string
		min       int
		max       int
		args      []string
		wantErr   bool
		errSubstr string
	}{
		{
			name:      "too many args returns error with max",
			min:       1,
			max:       3,
			args:      []string{"a", "b", "c", "d", "e"},
			wantErr:   true,
			errSubstr: "3",
		},
		{
			name:    "too few args for exact match",
			min:     2,
			max:     2,
			args:    []string{"a"},
			wantErr: true,
		},
		{
			name:    "valid number of args",
			min:     1,
			max:     3,
			args:    []string{"a", "b"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := cli.NewApp()
			set := flag.NewFlagSet("test", flag.ContinueOnError)
			set.Parse(tt.args)
			ctx := cli.NewContext(app, set, nil)

			err := checkNumberOfArguments(ctx, tt.min, tt.max)
			if tt.wantErr {
				if err == nil {
					t.Errorf("checkNumberOfArguments() expected error but got nil")
				} else if tt.errSubstr != "" && !strings.Contains(err.Error(), tt.errSubstr) {
					t.Errorf("checkNumberOfArguments() error = %q, want substring %q", err.Error(), tt.errSubstr)
				}
			} else {
				if err != nil {
					t.Errorf("checkNumberOfArguments() unexpected error: %v", err)
				}
			}
		})
	}
}
