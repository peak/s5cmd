package command

import (
	"fmt"
	"strings"

	"github.com/urfave/cli/v3"
)

// genericString reads a cli.GenericFlag's value as a string. Neither of v3's
// obvious accessors works here: cmd.String type-asserts to string, and
// cmd.Generic type-asserts to cli.Value, which the stored value does not
// satisfy because it is held by value while EnumValue's Set has a pointer
// receiver. Both return the zero value silently, which would quietly disable
// --log, --set, --metadata-directive and --structure.
func genericString(c *cli.Command, name string) string {
	switch v := c.Value(name).(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	default:
		return ""
	}
}

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

func (e EnumValue) Get() any {
	return e
}

type MapValue map[string]string

func (m MapValue) String() string {
	if m == nil {
		m = make(map[string]string)
	}

	var s strings.Builder
	for key, value := range m {
		s.WriteString(fmt.Sprintf("%s=%s ", key, value))
	}

	return s.String()
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
		return fmt.Errorf("key %q is already defined", key)
	}

	m[key] = value
	return nil
}

func (m MapValue) Get() any {
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

// The v3 Flag interface replaced Apply(*flag.FlagSet) with a Set/PreParse/
// PostParse lifecycle, so the plumbing below differs from the v2 version even
// though the flag behaves the same. The duplicate-key and "key=value" format
// checks live in MapValue.Set and are the reason for keeping a custom flag
// rather than switching to v3's StringMapFlag, which accepts both silently.

// PreParse initialises the backing map before any values arrive.
func (f *MapFlag) PreParse() error {
	if f.Value == nil {
		f.Value = make(MapValue)
	}
	return nil
}

// PostParse is a no-op: there is no default to apply and no environment
// variable to read for this flag.
func (f *MapFlag) PostParse() error {
	return nil
}

// Set records one "key=value" occurrence of the flag.
func (f *MapFlag) Set(_, value string) error {
	if f.Value == nil {
		f.Value = make(MapValue)
	}
	if err := f.Value.Set(value); err != nil {
		return err
	}
	f.HasBeenSet = true
	return nil
}

func (f *MapFlag) Get() any {
	if f.Value == nil {
		return make(MapValue)
	}
	return f.Value
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
	return !f.Hidden
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

// IsDefaultVisible reports whether help should print a default; this flag has
// no meaningful default to show.
func (f *MapFlag) IsDefaultVisible() bool {
	return false
}

// TypeName is used by help rendering to describe the expected value.
func (f *MapFlag) TypeName() string {
	return "key=value"
}

// IsMultiValueFlag reports that the flag may be repeated.
func (f *MapFlag) IsMultiValueFlag() bool {
	return true
}

func (f *MapFlag) GetCategory() string {
	return f.Category
}

func (f *MapFlag) SetCategory(c string) {
	f.Category = c
}

func (f *MapFlag) IsRequired() bool {
	return f.Required
}
