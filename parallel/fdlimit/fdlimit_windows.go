//go:build windows
// +build windows

package fdlimit

func Raise() error { return nil }
