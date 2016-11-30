package s5cmd

import (
	"fmt"
	"sort"
	"strings"
)

type Operation int

const (
	OP_ABORT Operation = iota
	OP_DOWNLOAD
	OP_BATCH_DOWNLOAD
	OP_UPLOAD
	OP_BATCH_UPLOAD
	OP_COPY
	OP_DELETE
	OP_SIZE
	OP_BATCH_DELETE        // "ls" and submit batched multi-delete operations
	OP_BATCH_DELETE_ACTUAL // Amazon deleteObjects call
	OP_LIST
	OP_LISTBUCKETS
	OP_LOCAL_COPY
	OP_LOCAL_DELETE
	OP_SHELL_EXEC
)

type ParamType int

const (
	PARAM_UNCHECKED ParamType = iota
	PARAM_UNCHECKED_ONE_OR_MORE
	PARAM_S3OBJ      // Bucket or bucket + key
	PARAM_S3DIR      // Bucket or bucket + key + "/" (prefix)
	PARAM_S3OBJORDIR // Bucket or bucket + key [+ "/"]
	PARAM_S3WILDOBJ  // Bucket + key with wildcard
	PARAM_FILEOBJ    // Filename
	PARAM_DIR        // Dir name or non-existing name ("/" appended)
	PARAM_FILEORDIR  // File or directory (if existing directory, "/" appended)
	PARAM_GLOB       // String containing a valid glob pattern
)

type OptionType int
type OptionList []OptionType

const (
	OPT_DELETE_SOURCE OptionType = iota + 1
	OPT_IF_NOT_EXISTS
	OPT_PARENTS // Just like cp --parents
	OPT_RR      // Reduced-redundancy
	OPT_IA      // Infrequent-access
)

type commandMap struct {
	keyword   string
	operation Operation
	params    []ParamType
	opts      OptionList
}

var commands = []commandMap{
	{"exit", OP_ABORT, []ParamType{}, OptionList{}},
	{"exit", OP_ABORT, []ParamType{PARAM_UNCHECKED}, OptionList{}},

	//{"get", OP_DOWNLOAD, []ParamType{PARAM_S3OBJ}, OptionList{}},
	//{"get", OP_BATCH_DOWNLOAD, []ParamType{PARAM_S3WILDOBJ}, OptionList{}},

	// File to file
	{"cp", OP_LOCAL_COPY, []ParamType{PARAM_FILEOBJ, PARAM_FILEORDIR}, OptionList{}},

	// S3 to S3
	{"cp", OP_COPY, []ParamType{PARAM_S3OBJ, PARAM_S3OBJORDIR}, OptionList{}},

	// File to S3
	{"cp", OP_UPLOAD, []ParamType{PARAM_FILEOBJ, PARAM_S3OBJORDIR}, OptionList{}},
	{"cp", OP_BATCH_UPLOAD, []ParamType{PARAM_GLOB, PARAM_S3DIR}, OptionList{}},
	{"cp", OP_BATCH_UPLOAD, []ParamType{PARAM_DIR, PARAM_S3DIR}, OptionList{}},

	// S3 to file
	{"cp", OP_DOWNLOAD, []ParamType{PARAM_S3OBJ, PARAM_FILEORDIR}, OptionList{}},
	{"cp", OP_BATCH_DOWNLOAD, []ParamType{PARAM_S3WILDOBJ, PARAM_DIR}, OptionList{}},

	// File to file
	{"mv", OP_LOCAL_COPY, []ParamType{PARAM_FILEOBJ, PARAM_FILEORDIR}, OptionList{OPT_DELETE_SOURCE}},

	// S3 to S3
	{"mv", OP_COPY, []ParamType{PARAM_S3OBJ, PARAM_S3OBJORDIR}, OptionList{OPT_DELETE_SOURCE}},

	// File to S3
	{"mv", OP_UPLOAD, []ParamType{PARAM_FILEOBJ, PARAM_S3OBJORDIR}, OptionList{OPT_DELETE_SOURCE}},
	{"mv", OP_BATCH_UPLOAD, []ParamType{PARAM_GLOB, PARAM_S3DIR}, OptionList{OPT_DELETE_SOURCE}},
	{"mv", OP_BATCH_UPLOAD, []ParamType{PARAM_DIR, PARAM_S3DIR}, OptionList{OPT_DELETE_SOURCE}},

	// S3 to file
	{"mv", OP_DOWNLOAD, []ParamType{PARAM_S3OBJ, PARAM_FILEORDIR}, OptionList{OPT_DELETE_SOURCE}},
	{"mv", OP_BATCH_DOWNLOAD, []ParamType{PARAM_S3WILDOBJ, PARAM_DIR}, OptionList{OPT_DELETE_SOURCE}},

	// File
	{"rm", OP_LOCAL_DELETE, []ParamType{PARAM_FILEOBJ}, OptionList{}},

	// S3
	{"rm", OP_DELETE, []ParamType{PARAM_S3OBJ}, OptionList{}},
	{"rm", OP_BATCH_DELETE, []ParamType{PARAM_S3WILDOBJ}, OptionList{}},
	{"batch-rm", OP_BATCH_DELETE_ACTUAL, []ParamType{PARAM_S3OBJ, PARAM_UNCHECKED_ONE_OR_MORE}, OptionList{}},

	{"ls", OP_LISTBUCKETS, []ParamType{}, OptionList{}},
	{"ls", OP_LIST, []ParamType{PARAM_S3OBJORDIR}, OptionList{}},
	{"ls", OP_LIST, []ParamType{PARAM_S3WILDOBJ}, OptionList{}},

	{"du", OP_SIZE, []ParamType{PARAM_S3OBJORDIR}, OptionList{}},
	{"du", OP_SIZE, []ParamType{PARAM_S3WILDOBJ}, OptionList{}},

	{"!", OP_SHELL_EXEC, []ParamType{PARAM_UNCHECKED_ONE_OR_MORE}, OptionList{}},
}

// Does this operation create sub-jobs?
func (o Operation) IsBatch() bool {
	return o == OP_BATCH_DOWNLOAD || o == OP_BATCH_UPLOAD || o == OP_BATCH_DELETE
}

// Internal operations are not shown in +OK messages
func (o Operation) IsInternal() bool {
	return o == OP_BATCH_DELETE_ACTUAL
}

func (o Operation) String() string {
	switch o {
	case OP_ABORT:
		return "abort"
	case OP_DOWNLOAD:
		return "download"
	case OP_BATCH_DOWNLOAD:
		return "batch-download"
	case OP_UPLOAD:
		return "upload"
	case OP_BATCH_UPLOAD:
		return "batch-upload"
	case OP_COPY:
		return "copy"
	case OP_DELETE:
		return "delete"
	case OP_BATCH_DELETE:
		return "batch-delete"
	case OP_BATCH_DELETE_ACTUAL:
		return "batch-delete-actual"
	case OP_LISTBUCKETS:
		return "ls-buckets"
	case OP_LIST:
		return "ls"
	case OP_SIZE:
		return "du"
	case OP_LOCAL_COPY:
		return "local-copy"
	case OP_LOCAL_DELETE:
		return "local-delete"
	case OP_SHELL_EXEC:
		return "shell-exec"
	}

	return fmt.Sprintf("Unknown:%d", o)
}

func (o Operation) Describe(l OptionList) string {
	switch o {
	case OP_ABORT:
		return "Exit program"
	case OP_DOWNLOAD:
		if l.Has(OPT_DELETE_SOURCE) {
			return "Download from S3 and delete source objects"
		}
		return "Download from S3"
	case OP_BATCH_DOWNLOAD:
		if l.Has(OPT_DELETE_SOURCE) {
			return "Batch download from S3 and delete source objects"
		}
		return "Batch download from S3"
	case OP_UPLOAD:
		if l.Has(OPT_DELETE_SOURCE) {
			return "Upload to S3 and delete source files"
		}
		return "Upload to S3"
	case OP_BATCH_UPLOAD:
		if l.Has(OPT_DELETE_SOURCE) {
			return "Batch upload to S3 and delete source files"
		}
		return "Batch upload to S3"
	case OP_COPY:
		if l.Has(OPT_DELETE_SOURCE) {
			return "Move S3 object"
		}
		return "Copy S3 object"
	case OP_DELETE:
		return "Delete from S3"
	case OP_BATCH_DELETE:
		return "Batch delete from S3"
	case OP_LISTBUCKETS:
		return "List buckets"
	case OP_LIST:
		return "List objects"
	case OP_SIZE:
		return "Count objects and size"
	case OP_LOCAL_COPY:
		if l.Has(OPT_DELETE_SOURCE) {
			return "Move local files"
		}
		return "Copy local files"
	case OP_LOCAL_DELETE:
		return "Delete local files"
	case OP_SHELL_EXEC:
		return "Arbitrary shell-execute"
	}

	return fmt.Sprintf("Unknown:%d", o)
}

func (o OptionList) Has(check OptionType) bool {
	for _, i := range o {
		if i == check {
			return true
		}
	}
	return false
}

func (o OptionType) GetParam() string {
	switch o {
	case OPT_IF_NOT_EXISTS:
		return "-n"
	case OPT_PARENTS:
		return "--parents"
	case OPT_RR:
		return "-rr"
	case OPT_IA:
		return "-ia"
	}
	return ""
}
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

func (o Operation) GetAcceptedOpts() *OptionList {
	l := OptionList{}

	switch o {
	case OP_DOWNLOAD, OP_UPLOAD, OP_COPY, OP_LOCAL_COPY, OP_BATCH_DOWNLOAD, OP_BATCH_UPLOAD:
		l = append(l, OPT_IF_NOT_EXISTS)
	}

	switch o {
	case OP_BATCH_DOWNLOAD, OP_BATCH_UPLOAD:
		l = append(l, OPT_PARENTS)
	}

	switch o {
	case OP_UPLOAD, OP_BATCH_UPLOAD, OP_COPY:
		l = append(l, OPT_RR, OPT_IA)
	}

	return &l
}

func (p ParamType) String() string {
	switch p {
	case PARAM_UNCHECKED:
		return "param"
	case PARAM_UNCHECKED_ONE_OR_MORE:
		return "param..."
	case PARAM_S3OBJ:
		return "s3://bucket[/object]"
	case PARAM_S3DIR:
		return "s3://bucket[/object]/"
	case PARAM_S3OBJORDIR:
		return "s3://bucket[/object[/]]"
	case PARAM_S3WILDOBJ:
		return "s3://bucket/wild/*/obj*"
	case PARAM_FILEOBJ:
		return "filename"
	case PARAM_DIR:
		return "directory"
	case PARAM_FILEORDIR:
		return "file-or-directory"
	case PARAM_GLOB:
		return "glob-pattern*"
	default:
		return "unknown"
	}
}

func GetCommandList() string {

	list := make(map[string][]string, 0)
	overrides := map[Operation]string{
		OP_ABORT:      "exit [exit code]",
		OP_SHELL_EXEC: "! command [parameters...]",
	}

	var lastDesc string
	var l []string
	for _, c := range commands {
		if c.operation.IsInternal() {
			continue
		}
		desc := c.operation.Describe(c.opts)
		if lastDesc != desc {
			if len(l) > 0 {
				list[lastDesc] = l
			}
			l = nil
			lastDesc = desc
		}

		if override, ok := overrides[c.operation]; ok {
			list[desc] = []string{override}
			lastDesc = ""
			l = nil
			continue
		}

		s := c.keyword
		ao := c.operation.GetAcceptedOpts()
		for _, p := range *ao {
			s += " [" + p.GetParam() + "]"
		}
		for _, pt := range c.params {
			s += " " + pt.String()
		}
		s = strings.Replace(s, " [-rr] [-ia] ", " [-rr|-ia] ", -1)
		l = append(l, s)
	}
	if len(l) > 0 {
		list[lastDesc] = l
	}

	// sort and build final string
	klist := make([]string, len(list))
	i := 0
	for k := range list {
		klist[i] = k
		i++
	}
	sort.Strings(klist)

	ret := ""
	for _, k := range klist {
		ret += "  " + k + "\n"
		for _, o := range list[k] {
			ret += "        " + o + "\n"
		}
	}

	return ret
}
