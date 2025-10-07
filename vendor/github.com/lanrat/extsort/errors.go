package extsort

import (
	"fmt"
)

// SerializationError represents an error that occurred during item serialization (ToBytes)
type SerializationError struct {
	// Cause is the original panic or error that occurred during serialization
	Cause interface{}
	// Context provides additional information about what was being serialized
	Context string
}

func (e *SerializationError) Error() string {
	if e.Context != "" {
		return fmt.Sprintf("serialization panic in %s: %v", e.Context, e.Cause)
	}
	return fmt.Sprintf("serialization panic: %v", e.Cause)
}

func (e *SerializationError) Unwrap() error {
	if err, ok := e.Cause.(error); ok {
		return err
	}
	return nil
}

// NewSerializationError creates a SerializationError
func NewSerializationError(cause interface{}, context string) error {
	return &SerializationError{Cause: cause, Context: context}
}

// DeserializationError represents an error that occurred during item deserialization (FromBytes)
type DeserializationError struct {
	// Cause is the original panic or error that occurred during deserialization
	Cause interface{}
	// DataSize is the size of the data that failed to deserialize
	DataSize int
	// Context provides additional information about what was being deserialized
	Context string
}

func (e *DeserializationError) Error() string {
	if e.Context != "" {
		return fmt.Sprintf("deserialization panic in %s (data size: %d bytes): %v", e.Context, e.DataSize, e.Cause)
	}
	return fmt.Sprintf("deserialization panic (data size: %d bytes): %v", e.DataSize, e.Cause)
}

func (e *DeserializationError) Unwrap() error {
	if err, ok := e.Cause.(error); ok {
		return err
	}
	return nil
}

// NewDeserializationError creates a DeserializationError
func NewDeserializationError(cause interface{}, dataSize int, context string) error {
	return &DeserializationError{Cause: cause, DataSize: dataSize, Context: context}
}

// ComparisonError represents an error that occurred during item comparison
type ComparisonError struct {
	// Cause is the original panic or error that occurred during comparison
	Cause interface{}
	// Context provides additional information about when the comparison failed
	Context string
}

func (e *ComparisonError) Error() string {
	if e.Context != "" {
		return fmt.Sprintf("comparison panic in %s: %v", e.Context, e.Cause)
	}
	return fmt.Sprintf("comparison panic: %v", e.Cause)
}

func (e *ComparisonError) Unwrap() error {
	if err, ok := e.Cause.(error); ok {
		return err
	}
	return nil
}

// NewComparisonError creates a ComparisonError
func NewComparisonError(cause interface{}, context string) error {
	return &ComparisonError{Cause: cause, Context: context}
}

// NewDiskError creates a DiskError wrapping the underlying I/O error
func NewDiskError(err error, operation, path string) error {
	if path != "" {
		return fmt.Errorf("disk error during %s on %s: %w", operation, path, err)
	}
	return fmt.Errorf("disk error during %s: %w", operation, err)
}

// ConfigError represents an error in configuration parameters
type ConfigError struct {
	// Field is the name of the configuration field that's invalid
	Field string
	// Value is the invalid value provided
	Value interface{}
	// Reason explains why the value is invalid
	Reason string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("config error in field %s (value: %v): %s", e.Field, e.Value, e.Reason)
}

// NewResourceError creates a ResourceError wrapping the underlying error
func NewResourceError(err error, resource, context string) error {
	if context != "" {
		return fmt.Errorf("resource error (%s) in %s: %w", resource, context, err)
	}
	return fmt.Errorf("resource error (%s): %w", resource, err)
}
