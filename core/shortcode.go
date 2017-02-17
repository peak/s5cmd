package core

type shortCode int

const (
	shortErr = iota
	shortOk
	shortInfo
	shortOkWithError
)

func (s shortCode) String() string {
	if s == shortOk {
		return "+"
	}
	if s == shortErr {
		return "-"
	}
	if s == shortInfo {
		return "#"
	}
	if s == shortOkWithError {
		return "+?"
	}
	return "?"
}
