package hash

import (
	"bytes"
	"testing"
)

func Test_checkMultipart(t *testing.T) {
	t.Parallel()
	type args struct {
		hashValue string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "empty string",
			args: args{hashValue: ""},
			want: 0,
		},
		{
			name: "normal file hash",
			args: args{hashValue: "811277107d55c60ea0a2f86609a7088c"},
			want: 0,
		},
		{
			name: "multipart uploaded file hash",
			args: args{hashValue: "811277107d55c60ea0a2f86609a7088c-3"},
			want: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := checkMultipart(tt.args.hashValue); got != tt.want {
				t.Errorf("checkMultipart() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_fileToHash(t *testing.T) {
	t.Parallel()
	type args struct {
		fileContent string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name:    "empty string",
			args:    args{fileContent: ""},
			want:    "d41d8cd98f00b204e9800998ecf8427e",
			wantErr: false,
		},
		{
			name:    "normal file",
			args:    args{fileContent: "this is a readme file"},
			want:    "80322a4a12e357e9bafbaf5809537f27",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			buffer.WriteString(tt.args.fileContent)
			got, err := fileToHash(&buffer)
			if (err != nil) != tt.wantErr {
				t.Errorf("fileToHash() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("fileToHash() = %v, want %v", got, tt.want)
			}
		})
	}
}
