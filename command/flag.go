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

type MapValue struct {
	m map[string]string
}

func NewMapValue() *MapValue {
	return &MapValue{
		m: map[string]string{},
	}
}

func (m *MapValue) String() string {
	s := ""

	for key, value := range m.m {
		s = fmt.Sprintf("%s, %s=%s, ", s, key, value)
	}

	return s
}

func (m *MapValue) Set(s string) error {
	tokens := strings.Split(s, "=")

	if len(tokens) > 2 {
		return fmt.Errorf("the key value pair(%s) format is invalid", tokens)
	}

	_, ok := m.m[tokens[0]]
	if ok {
		return fmt.Errorf("the key: %s is already defined", tokens[0])
	}

	m.m[tokens[0]] = tokens[1]

	return nil
}

func (m *MapValue) Get() interface{} {
	return m.m
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

	M *MapValue
}

func (f *MapFlag) Apply(set *flag.FlagSet) error {
	f.M = NewMapValue()
	for _, name := range f.Names() {
		set.Var(f.M, name, f.Usage)
		if len(f.M.m) > 0 {
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
	return f.M.String()
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

func (f *MapFlag) Get(cCtx *cli.Context) map[string]*string {
	val, ok := cCtx.Generic(f.Name).(map[string]*string)
	if !ok {
		return nil
	}
	return val
}
