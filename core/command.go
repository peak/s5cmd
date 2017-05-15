package core

import (
	"sort"
	"strings"

	"github.com/peakgames/s5cmd/op"
	"github.com/peakgames/s5cmd/opt"
)

// CommandMap describes each command
type CommandMap struct {
	// Keyword is the command's invocation name
	Keyword string
	// Operation is the operation to invoke when this command runs
	Operation op.Operation
	// Params are the accepted parameter types
	Params []opt.ParamType
	// Opts are the options to invoke the operation with, when this command is run
	Opts opt.OptionList
}

// Commands is a list of registered commands
var Commands = []CommandMap{
	{"exit", op.Abort, []opt.ParamType{}, opt.OptionList{}},
	{"exit", op.Abort, []opt.ParamType{opt.Unchecked}, opt.OptionList{}},

	//{"get", op.Download, []opt.ParamType{opt.S3Obj}, opt.OptionList{}},
	//{"get", op.BatchDownload, []opt.ParamType{opt.S3WildObj}, opt.OptionList{}},

	// File to file
	{"cp", op.LocalCopy, []opt.ParamType{opt.FileObj, opt.FileOrDir}, opt.OptionList{}},
	{"cp", op.BatchLocalCopy, []opt.ParamType{opt.Glob, opt.Dir}, opt.OptionList{}},
	{"cp", op.BatchLocalCopy, []opt.ParamType{opt.Dir, opt.Dir}, opt.OptionList{}},

	// S3 to S3
	{"cp", op.Copy, []opt.ParamType{opt.S3Obj, opt.S3ObjOrDir}, opt.OptionList{}},
	{"cp", op.BatchCopy, []opt.ParamType{opt.S3WildObj, opt.S3Dir}, opt.OptionList{}},

	// File to S3
	{"cp", op.Upload, []opt.ParamType{opt.FileObj, opt.S3ObjOrDir}, opt.OptionList{}},
	{"cp", op.BatchUpload, []opt.ParamType{opt.Glob, opt.S3Dir}, opt.OptionList{}},
	{"cp", op.BatchUpload, []opt.ParamType{opt.Dir, opt.S3Dir}, opt.OptionList{}},

	// S3 to file
	{"cp", op.Download, []opt.ParamType{opt.S3Obj, opt.FileOrDir}, opt.OptionList{}},
	{"cp", op.BatchDownload, []opt.ParamType{opt.S3WildObj, opt.Dir}, opt.OptionList{}},

	// File to file
	{"mv", op.LocalCopy, []opt.ParamType{opt.FileObj, opt.FileOrDir}, opt.OptionList{opt.DeleteSource}},
	{"mv", op.BatchLocalCopy, []opt.ParamType{opt.Glob, opt.Dir}, opt.OptionList{opt.DeleteSource}},
	{"mv", op.BatchLocalCopy, []opt.ParamType{opt.Dir, opt.Dir}, opt.OptionList{opt.DeleteSource}},

	// S3 to S3
	{"mv", op.Copy, []opt.ParamType{opt.S3Obj, opt.S3ObjOrDir}, opt.OptionList{opt.DeleteSource}},
	{"mv", op.BatchCopy, []opt.ParamType{opt.S3WildObj, opt.S3Dir}, opt.OptionList{opt.DeleteSource}},

	// File to S3
	{"mv", op.Upload, []opt.ParamType{opt.FileObj, opt.S3ObjOrDir}, opt.OptionList{opt.DeleteSource}},
	{"mv", op.BatchUpload, []opt.ParamType{opt.Glob, opt.S3Dir}, opt.OptionList{opt.DeleteSource}},
	{"mv", op.BatchUpload, []opt.ParamType{opt.Dir, opt.S3Dir}, opt.OptionList{opt.DeleteSource}},

	// S3 to file
	{"mv", op.Download, []opt.ParamType{opt.S3Obj, opt.FileOrDir}, opt.OptionList{opt.DeleteSource}},
	{"mv", op.BatchDownload, []opt.ParamType{opt.S3WildObj, opt.Dir}, opt.OptionList{opt.DeleteSource}},

	// File
	{"rm", op.LocalDelete, []opt.ParamType{opt.FileObj}, opt.OptionList{}},

	// S3
	{"rm", op.Delete, []opt.ParamType{opt.S3Obj}, opt.OptionList{}},
	{"rm", op.BatchDelete, []opt.ParamType{opt.S3WildObj}, opt.OptionList{}},
	{"batch-rm", op.BatchDeleteActual, []opt.ParamType{opt.S3Obj, opt.UncheckedOneOrMore}, opt.OptionList{}},

	{"ls", op.ListBuckets, []opt.ParamType{}, opt.OptionList{}},
	{"ls", op.List, []opt.ParamType{opt.S3ObjOrDir}, opt.OptionList{}},
	{"ls", op.List, []opt.ParamType{opt.S3WildObj}, opt.OptionList{}},

	{"du", op.Size, []opt.ParamType{opt.S3ObjOrDir}, opt.OptionList{}},
	{"du", op.Size, []opt.ParamType{opt.S3WildObj}, opt.OptionList{}},

	{"!", op.ShellExec, []opt.ParamType{opt.UncheckedOneOrMore}, opt.OptionList{}},
}

// String formats the CommandMap using its Operation and ParamTypes
func (c *CommandMap) String(optsOverride ...opt.OptionType) (s string) {
	s = c.Operation.String() + " (" + c.Keyword + ")"

	if len(optsOverride) > 0 {
		s += " {Opts:"
		for _, o := range optsOverride {
			s += " " + o.GetParam()
		}
		s += "}"
	} else if len(c.Opts) > 0 {
		s += " {default Opts:"
		for _, o := range c.Opts {
			s += " " + o.GetParam()
		}
		s += "}"
	}

	for _, p := range c.Params {
		s += " [" + p.String() + "]"
	}

	return
}

// GetCommandList returns a text of accepted Commands with their options and arguments
func GetCommandList() string {
	list := make(map[string][]string, 0)
	overrides := map[op.Operation]string{
		op.Abort:     "exit [exit code]",
		op.ShellExec: "! command [parameters...]",
	}

	var lastDesc string
	var l []string
	for _, c := range Commands {
		if c.Operation.IsInternal() {
			continue
		}
		desc := c.Operation.Describe(c.Opts)
		if lastDesc != desc {
			if len(l) > 0 {
				list[lastDesc] = l
			}
			l = nil
			lastDesc = desc
		}

		if override, ok := overrides[c.Operation]; ok {
			list[desc] = []string{override}
			lastDesc = ""
			l = nil
			continue
		}

		s := c.Keyword
		ao := c.Operation.GetAcceptedOpts()
		for _, p := range *ao {
			s += " [" + p.GetParam() + "]"
		}
		for _, pt := range c.Params {
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
