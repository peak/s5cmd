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
