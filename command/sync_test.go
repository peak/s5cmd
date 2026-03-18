package command

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
)

func TestShouldStopSync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		err         error
		exitOnError bool
		want        bool
	}{
		{
			name: "AccessDenied",
			err:  awserr.New("AccessDenied", "access denied", nil),
			want: true,
		},
		{
			name: "NoSuchBucket",
			err:  awserr.New("NoSuchBucket", "no such bucket", nil),
			want: true,
		},
		{
			name: "RequestError",
			err:  awserr.New("RequestError", "request error", nil),
			want: true,
		},
		{
			name: "SerializationError",
			err:  awserr.New("SerializationError", "serialization error", nil),
			want: true,
		},
		{
			name: "SlowDown should not stop",
			err:  awserr.New("SlowDown", "slow down", nil),
			want: false,
		},
		{
			name:        "non-AWS error with exitOnError true",
			err:         fmt.Errorf("some random error"),
			exitOnError: true,
			want:        true,
		},
		{
			name:        "non-AWS error with exitOnError false",
			err:         fmt.Errorf("some random error"),
			exitOnError: false,
			want:        false,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := Sync{
				exitOnError: tc.exitOnError,
			}

			got := s.shouldStopSync(tc.err)
			if got != tc.want {
				t.Errorf("shouldStopSync(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
