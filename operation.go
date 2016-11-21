package s5cmd

import "fmt"

type Operation int

const (
	OP_ABORT Operation = iota
	OP_DOWNLOAD
	OP_BATCH_DOWNLOAD
	OP_UPLOAD
	OP_BATCH_UPLOAD
	OP_COPY
	OP_MOVE
	OP_DELETE
	OP_BATCH_DELETE        // "ls" and submit batched multi-delete operations
	OP_BATCH_DELETE_ACTUAL // Amazon deleteObjects call
	OP_LIST
	OP_LISTBUCKETS
	OP_LOCAL_COPY
	OP_LOCAL_MOVE
	OP_LOCAL_DELETE
	OP_SHELL_EXEC
)

type ParamType int

const (
	PARAM_UNCHECKED ParamType = iota
	PARAM_UNCHECKED_ONE_OR_MORE
	PARAM_S3OBJ
	PARAM_S3OBJORDIR
	PARAM_S3DIR
	PARAM_S3WILDOBJ
	PARAM_FILEOBJ
	PARAM_FILEORDIR
	PARAM_DIR
	PARAM_GLOB
)

type commandMap struct {
	keyword   string
	operation Operation
	params    []ParamType
}

var commands = []commandMap{
	{"exit", OP_ABORT, []ParamType{}},
	{"exit", OP_ABORT, []ParamType{PARAM_UNCHECKED}},
	{"get", OP_DOWNLOAD, []ParamType{PARAM_S3OBJ}},
	{"get", OP_DOWNLOAD, []ParamType{PARAM_S3OBJ, PARAM_FILEORDIR}},
	{"get", OP_BATCH_DOWNLOAD, []ParamType{PARAM_S3WILDOBJ}},
	{"get", OP_BATCH_DOWNLOAD, []ParamType{PARAM_S3WILDOBJ, PARAM_DIR}},
	{"put", OP_UPLOAD, []ParamType{PARAM_FILEOBJ, PARAM_S3OBJORDIR}},
	{"put", OP_BATCH_UPLOAD, []ParamType{PARAM_GLOB, PARAM_S3DIR}},
	{"put", OP_BATCH_UPLOAD, []ParamType{PARAM_DIR, PARAM_S3DIR}},
	{"cp", OP_COPY, []ParamType{PARAM_S3OBJ, PARAM_S3OBJORDIR}},
	{"mv", OP_MOVE, []ParamType{PARAM_S3OBJ, PARAM_S3OBJORDIR}},
	{"rm", OP_DELETE, []ParamType{PARAM_S3OBJ}},
	{"rm", OP_BATCH_DELETE, []ParamType{PARAM_S3WILDOBJ}},
	{"batch-rm", OP_BATCH_DELETE_ACTUAL, []ParamType{PARAM_S3OBJ, PARAM_UNCHECKED_ONE_OR_MORE}},
	{"ls", OP_LISTBUCKETS, []ParamType{}},
	{"ls", OP_LIST, []ParamType{PARAM_S3OBJORDIR}},
	{"ls", OP_LIST, []ParamType{PARAM_S3WILDOBJ}},
	{"!cp", OP_LOCAL_COPY, []ParamType{PARAM_FILEOBJ, PARAM_FILEORDIR}},
	{"!mv", OP_LOCAL_MOVE, []ParamType{PARAM_FILEOBJ, PARAM_FILEORDIR}},
	{"!rm", OP_LOCAL_DELETE, []ParamType{PARAM_FILEOBJ}},
	{"!", OP_SHELL_EXEC, []ParamType{PARAM_UNCHECKED_ONE_OR_MORE}},
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
	case OP_MOVE:
		return "move"
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
	case OP_LOCAL_COPY:
		return "local-copy"
	case OP_LOCAL_MOVE:
		return "local-move"
	case OP_LOCAL_DELETE:
		return "local-delete"
	case OP_SHELL_EXEC:
		return "shell-exec"
	}

	return fmt.Sprintf("Unknown:%d", o)
}
