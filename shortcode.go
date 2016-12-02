package main

type shortCode int

const (
	shortErr = iota
	shortOk
	shortInfo
)

func (s shortCode) String() string {
	if s == shortOk {
		return "+"
	}
	if s == shortErr {
		return "-"
	}
	if s == shortInfo {
		return "?"
	}
	return "?"
}
