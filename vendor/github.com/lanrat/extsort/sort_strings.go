package extsort

import "cmp"

// StringSorter provides external sorting for strings, maintaining backward compatibility
// with the legacy string-specific API. It embeds GenericSorter[string] and uses
// simple byte slice conversion for serialization.
type StringSorter struct {
	GenericSorter[string]
}

// fromBytesString converts a byte slice back to a string.
// This is used for deserialization during the external sort process.
// Always succeeds and returns nil error.
func fromBytesString(d []byte) (string, error) {
	return string(d), nil
}

// toBytesString converts a string to a byte slice for serialization.
// This enables strings to be written to and read from temporary files during sorting.
// Always succeeds and returns nil error.
func toBytesString(s string) ([]byte, error) {
	return []byte(s), nil
}

// Strings performs external sorting on a channel of strings using lexicographic ordering.
// Returns the sorter instance, output channel with sorted strings, and error channel.
// This function provides backward compatibility with the legacy string-specific API.
func Strings(input <-chan string, config *Config) (*StringSorter, <-chan string, <-chan error) {
	genericSorter, output, errChan := Generic(input, fromBytesString, toBytesString, cmp.Compare, config)
	if genericSorter == nil {
		return nil, output, errChan
	}
	s := &StringSorter{GenericSorter: *genericSorter}
	return s, output, errChan
}

// StringsMock performs external sorting on strings with a mock implementation that limits
// the number of strings to sort. Useful for testing with a controlled dataset size.
// The parameter n specifies the maximum number of strings to process.
func StringsMock(input <-chan string, config *Config, n int) (*StringSorter, <-chan string, <-chan error) {
	genericSorter, output, errChan := MockGeneric(input, fromBytesString, toBytesString, cmp.Compare, config, n)
	if genericSorter == nil {
		return nil, output, errChan
	}
	s := &StringSorter{GenericSorter: *genericSorter}
	return s, output, errChan
}
