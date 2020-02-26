package core

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/peak/s5cmd/objurl"
	"github.com/peak/s5cmd/op"
	"github.com/peak/s5cmd/opt"
	"github.com/peak/s5cmd/storage"
)

// Command is the representation of the command.
type Command struct {
	// the original string received from user, such as "cp -h", or "cp -R foo bar"
	original  string
	keyword   string
	operation op.Operation
	args      []*objurl.ObjectURL
	opts      opt.OptionList
}

// String is the representation of command.
func (c Command) String() string {
	return c.original
}

// getStorageClass gets storage class from option list.
func (c Command) getStorageClass() storage.StorageClass {
	var cls storage.StorageClass
	if c.opts.Has(opt.RR) {
		cls = storage.StorageReducedRedundancy
	} else if c.opts.Has(opt.IA) {
		cls = storage.StorageStandardIA
	} else {
		cls = storage.StorageStandard
	}
	return cls
}

// SupportsAggregation checks if command is supports aggregation, such as S3 batch deletes.
func (c Command) SupportsAggregation() bool {
	return c.opts.Has(opt.SupportsAggregation)
}

// IsBatch checks if command is a batch operation.
func (c Command) IsBatch() bool {
	return c.operation.IsBatch()
}

// makeJob creates new Job from the command.
func (c Command) makeJob(cmd string, operation op.Operation, args ...*objurl.ObjectURL) *Job {
	return &Job{
		command:      cmd,
		operation:    operation,
		opts:         c.opts,
		args:         args,
		storageClass: c.getStorageClass(),
		statType:     operation.GetStat(),
	}
}

// toJob converts raw command to job.
func (c Command) toJob() *Job {
	return &Job{
		command:      c.keyword,
		operation:    c.operation,
		opts:         c.opts,
		args:         c.args,
		storageClass: c.getStorageClass(),
		statType:     c.operation.GetStat(),
	}
}

// displayHelp displays help text.
func (c Command) displayHelp() {
	fmt.Fprintf(os.Stderr, "%v\n\n", UsageLine())

	cl, opts, cnt := CommandHelps(c.keyword)

	if ol := opt.OptionHelps(opts); ol != "" {
		fmt.Fprintf(os.Stderr, "\"%v\" command options:\n", c)
		fmt.Fprint(os.Stderr, ol)
		fmt.Fprint(os.Stderr, "\n\n")
	}

	if cnt > 1 {
		fmt.Fprintf(os.Stderr, "Help for \"%v\" commands:\n", c)
	}
	fmt.Fprint(os.Stderr, cl)
	fmt.Fprint(os.Stderr, "\nTo list available general options, run without arguments.\n")
}

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

var noOpts = opt.OptionList{}

// Commands is a list of registered commands
var Commands = []CommandMap{
	{"exit", op.Abort, []opt.ParamType{}, noOpts},
	{"exit", op.Abort, []opt.ParamType{opt.Unchecked}, noOpts},

	// File to file
	{"cp", op.LocalCopy, []opt.ParamType{opt.FileObj, opt.FileOrDir}, noOpts},
	{"cp", op.BatchLocalCopy, []opt.ParamType{opt.Glob, opt.Dir}, noOpts},
	{"cp", op.BatchLocalCopy, []opt.ParamType{opt.Dir, opt.Dir}, noOpts},

	// S3 to S3
	{"cp", op.Copy, []opt.ParamType{opt.S3SimpleObj, opt.S3ObjOrDir}, noOpts},
	{"cp", op.BatchCopy, []opt.ParamType{opt.S3WildObj, opt.S3Dir}, noOpts},

	// File to S3
	{"cp", op.Upload, []opt.ParamType{opt.FileObj, opt.S3ObjOrDir}, noOpts},
	{"cp", op.BatchUpload, []opt.ParamType{opt.Glob, opt.S3Dir}, opt.OptionList{opt.Recursive}},
	{"cp", op.BatchUpload, []opt.ParamType{opt.Dir, opt.S3Dir}, opt.OptionList{opt.Recursive}},

	// S3 to file
	{"cp", op.Download, []opt.ParamType{opt.S3SimpleObj, opt.FileOrDir}, noOpts},
	{"get", op.AliasGet, []opt.ParamType{opt.S3SimpleObj, opt.OptionalFileOrDir}, noOpts},
	{"cp", op.BatchDownload, []opt.ParamType{opt.S3WildObj, opt.Dir}, noOpts},
	{"get", op.AliasBatchGet, []opt.ParamType{opt.S3WildObj, opt.OptionalDir}, noOpts},

	// File to file
	{"mv", op.LocalCopy, []opt.ParamType{opt.FileObj, opt.FileOrDir}, opt.OptionList{opt.DeleteSource}},
	{"mv", op.BatchLocalCopy, []opt.ParamType{opt.Glob, opt.Dir}, opt.OptionList{opt.DeleteSource}},
	{"mv", op.BatchLocalCopy, []opt.ParamType{opt.Dir, opt.Dir}, opt.OptionList{opt.DeleteSource}},

	// S3 to S3
	{"mv", op.Copy, []opt.ParamType{opt.S3SimpleObj, opt.S3ObjOrDir}, opt.OptionList{opt.DeleteSource}},
	{"mv", op.BatchCopy, []opt.ParamType{opt.S3WildObj, opt.S3Dir}, opt.OptionList{opt.DeleteSource}},

	// File to S3
	{"mv", op.Upload, []opt.ParamType{opt.FileObj, opt.S3ObjOrDir}, opt.OptionList{opt.DeleteSource}},
	{"mv", op.BatchUpload, []opt.ParamType{opt.Glob, opt.S3Dir}, opt.OptionList{opt.DeleteSource}},
	{"mv", op.BatchUpload, []opt.ParamType{opt.Dir, opt.S3Dir}, opt.OptionList{opt.DeleteSource}},

	// S3 to file
	{"mv", op.Download, []opt.ParamType{opt.S3SimpleObj, opt.FileOrDir}, opt.OptionList{opt.DeleteSource}},
	{"mv", op.BatchDownload, []opt.ParamType{opt.S3WildObj, opt.Dir}, opt.OptionList{opt.DeleteSource}},

	// File
	{"rm", op.LocalDelete, []opt.ParamType{opt.FileObj}, noOpts},

	// S3
	{"rm", op.Delete, []opt.ParamType{opt.S3SimpleObj}, noOpts},
	{"rm", op.BatchDelete, []opt.ParamType{opt.S3WildObj}, opt.OptionList{opt.SupportsAggregation}},
	{"batch-rm", op.BatchDelete, []opt.ParamType{opt.S3Obj, opt.UncheckedOneOrMore}, opt.OptionList{opt.SupportsAggregation}},

	{"ls", op.ListBuckets, []opt.ParamType{}, noOpts},
	{"ls", op.List, []opt.ParamType{opt.S3ObjOrDir}, noOpts},
	{"ls", op.List, []opt.ParamType{opt.S3WildObj}, noOpts},

	{"du", op.Size, []opt.ParamType{opt.S3ObjOrDir}, noOpts},
	{"du", op.Size, []opt.ParamType{opt.S3WildObj}, noOpts},

	{"!", op.ShellExec, []opt.ParamType{opt.UncheckedOneOrMore}, noOpts},
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

// CommandHelps returns a text of accepted Commands with their options and arguments, list of accepted options, and a count of command alternates
func CommandHelps(filter string) (string, []opt.OptionType, int) {
	list := make(map[string][]string)
	overrides := map[op.Operation]string{
		op.Abort:     "exit [exit code]",
		op.ShellExec: "! command [parameters...]",
	}

	optsList := make(map[opt.OptionType]struct{})

	var lastDesc string
	var l []string
	for _, c := range Commands {
		if filter != "" && c.Keyword != filter {
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
			if p == opt.Help {
				continue
			}
			optsList[p] = struct{}{}

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

	var optsUsed []opt.OptionType
	for k := range optsList {
		optsUsed = append(optsUsed, k)
	}

	return ret, optsUsed, len(list)
}

// CommandList returns a list of accepted Commands
func CommandList() []string {
	l := make(map[string]struct{})
	for _, c := range Commands {
		l[c.Keyword] = struct{}{}
	}

	var list []string

	for k := range l {
		list = append(list, k)
	}
	sort.Strings(list)

	return list
}

// UsageLine returns the generic usage line for s5cmd
func UsageLine() string {
	return fmt.Sprintf("Usage: %s [OPTION]... [COMMAND [PARAMS...]]", os.Args[0])
}
