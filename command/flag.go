package command

import (
	"flag"
	"fmt"
	"strings"

	"github.com/urfave/cli/v2"
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

type MapValue map[string]string

func (m MapValue) String() string {
	if m == nil {
		m = make(map[string]string)
	}
	s := ""

	for key, value := range m {
		s = fmt.Sprintf("%s, %s=%s, ", s, key, value)
	}

	return s
}

func (m MapValue) Set(s string) error {
	if m == nil {
		m = make(map[string]string)
	}

	if len(s) == 0 {
		return fmt.Errorf("flag can't be passed empty. Format: key=value")
	}

	tokens := strings.Split(s, "=")
	if len(tokens) <= 1 {
		return fmt.Errorf("the key value pair(%s) has invalid format", tokens)
	}

	key := tokens[0]
	value := strings.Join(tokens[1:], "=")

	_, ok := m[key]
	if ok {
		return fmt.Errorf("the key: %s is already defined", key)
	}

	m[key] = value
	return nil
}

func (m MapValue) Get() interface{} {
	if m == nil {
		m = make(map[string]string)
	}
	return m
}

type MapFlag struct {
	Name string

	Category    string
	DefaultText string
	FilePath    string
	Usage       string

	HasBeenSet bool
	Required   bool
	Hidden     bool

	Value MapValue
}

var (
	_ cli.Flag              = (*MapFlag)(nil)
	_ cli.RequiredFlag      = (*MapFlag)(nil)
	_ cli.VisibleFlag       = (*MapFlag)(nil)
	_ cli.DocGenerationFlag = (*MapFlag)(nil)
)

func (f *MapFlag) Apply(set *flag.FlagSet) error {
	if f.Value == nil {
		f.Value = make(map[string]string)
	}
	for _, name := range f.Names() {
		set.Var(f.Value, name, f.Usage)
		if len(f.Value) > 0 {
			f.HasBeenSet = true
		}
	}

	return nil
}

func (f *MapFlag) GetUsage() string {
	return f.Usage
}

func (f *MapFlag) Names() []string {
	return []string{f.Name}
}

func (f *MapFlag) IsSet() bool {
	return f.HasBeenSet
}

func (f *MapFlag) IsVisible() bool {
	return true
}

func (f *MapFlag) String() string {
	return cli.FlagStringer(f)
}

func (f *MapFlag) TakesValue() bool {
	return true
}

func (f *MapFlag) GetValue() string {
	return f.Value.String()
}

func (f *MapFlag) GetDefaultText() string {
	return ""
}

func (f *MapFlag) GetEnvVars() []string {
	return []string{}
}

func (f *MapFlag) IsRequired() bool {
	return f.Required
}
