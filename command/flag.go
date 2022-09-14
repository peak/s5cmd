package command

import (
	"fmt"
	"strings"
)

type EnumValue struct {
	Enum    []string
	Default string
	// ConditionFunction is used to check if the value passed to Set method is valid
	// or not.
	// If ConditionFunction is not set, it defaults to string '==' comparison.
	ConditionFunction func(str, target string) bool
	selected          string
}

func (e *EnumValue) Set(value string) error {
	if e.ConditionFunction == nil {
		e.ConditionFunction = func(str, target string) bool {
			return str == target
		}
	}
	for _, enum := range e.Enum {
		if e.ConditionFunction(enum, value) {
			e.selected = value
			return nil
		}
	}

	return fmt.Errorf("allowed values: [%s]", strings.Join(e.Enum, ", "))
}

func (e EnumValue) String() string {
	if e.selected == "" {
		return e.Default
	}
	return e.selected
}

func (e EnumValue) Get() interface{} {
	return e
}
