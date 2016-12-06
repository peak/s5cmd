// Package opt defines option and parameter types.
package opt

import "strings"

// OptionType is a type for our options. These can be provided with optional parameters or can be already set in commandMap
type OptionType int

// OptionList is a slice of OptionTypes
type OptionList []OptionType

const (
	DeleteSource OptionType = iota + 1 // Delete source file/object
	IfNotExists                        // Run only if destination does not exist
	Parents                            // Just like cp --parents
	RR                                 // Reduced-redundancy
	IA                                 // Infrequent-access
)

// Has determines if the opt.OptionList contains this OptionType
func (l OptionList) Has(check OptionType) bool {
	for _, i := range l {
		if i == check {
			return true
		}
	}
	return false
}

// GetParam returns the string/command parameter representation of a specific OptionType
func (o OptionType) GetParam() string {
	switch o {
	case IfNotExists:
		return "-n"
	case Parents:
		return "--parents"
	case RR:
		return "-rr"
	case IA:
		return "-ia"
	}
	return ""
}

// GetParams runs GetParam() on an opt.OptionList and returns a concatenated string
func (l OptionList) GetParams() string {
	r := make([]string, 0)
	for _, o := range l {
		p := o.GetParam()
		if p != "" {
			r = append(r, p)
		}
	}

	j := strings.Join(r, " ")
	if j != "" {
		return " " + j
	}
	return ""
}

// ParamType is the type of our parameter. Determines how we validate the arguments.
type ParamType int

const (
	Unchecked          ParamType = iota // Arbitrary single parameter
	UncheckedOneOrMore                  // One or more arbitrary parameters (special case)
	S3Obj                               // Bucket or bucket + key
	S3Dir                               // Bucket or bucket + key + "/" (prefix)
	S3ObjOrDir                          // Bucket or bucket + key [+ "/"]
	S3WildObj                           // Bucket + key with wildcard
	FileObj                             // Filename
	Dir                                 // Dir name or non-existing name ("/" appended)
	FileOrDir                           // File or directory (if existing directory, "/" appended)
	Glob                                // String containing a valid glob pattern
)

// String returns the string representation of ParamType
func (p ParamType) String() string {
	switch p {
	case Unchecked:
		return "param"
	case UncheckedOneOrMore:
		return "param..."
	case S3Obj:
		return "s3://bucket[/object]"
	case S3Dir:
		return "s3://bucket[/object]/"
	case S3ObjOrDir:
		return "s3://bucket[/object[/]]"
	case S3WildObj:
		return "s3://bucket/wild/*/obj*"
	case FileObj:
		return "filename"
	case Dir:
		return "directory"
	case FileOrDir:
		return "file-or-directory"
	case Glob:
		return "glob-pattern*"
	default:
		return "unknown"
	}
}
