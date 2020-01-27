![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)
![Tag](https://img.shields.io/github/tag/peak/s5cmd.svg)
[![godoc](https://img.shields.io/badge/godoc-reference-blue.svg)](https://godoc.org/github.com/peak/s5cmd)
[![Go Report](https://goreportcard.com/badge/github.com/peak/s5cmd)](https://goreportcard.com/report/github.com/peak/s5cmd)

# s5cmd #

This is a parallel S3 and local filesystem execution tool.

![demo1](./doc/recording1.gif)

![demo2](./doc/recording2.gif)

## Installation ##

You'll need [Go 1.13+](https://golang.org/dl) to install `s5cmd`.

    go get -u github.com/peak/s5cmd

This will install `s5cmd` in your `$GOPATH/bin` directory.

Using [Homebrew](https://brew.sh/):

    brew tap peak/s5cmd https://github.com/peak/s5cmd
    brew install s5cmd

### Shell Auto-Completion ###

Bash and zsh shell completion is supported, utilizing [posener/complete](https://github.com/posener/complete). Tool and subcommand parameters, local files/dirs, as well as remote (S3) buckets and objects are supported.

To enable auto-completion, run:
```
s5cmd -cmp-install
```
This will add a few lines (depending on configuration) to your `.bashrc` or `.zshrc` file. After installation, run `source .bashrc` (or restart your shell) to activate the changes.

> Note: Auto-completion always works with AWS Endpoint, `-endpoint-url` does not override it's behaviour.

#### Examples ####

To utilize shell autocompletion, use the `<tab>` key on the shell CLI. If there is more than one match, nothing will happen until a second `<tab>` is hit, which then will show options. Examples:

```
s5cmd <tab> # This is going to list commands and general parameters
s5cmd -n<tab> # This will autocomplete to "s5cmd -numworkers"
s5cmd -numworkers <tab><tab> # This will recommend some values for numworkers
s5cmd ls <tab><tab> # This will recommend options for the "ls" command or an "s3://" prefix
s5cmd ls s3://<tab><tab> # This will get a bucket list from S3
s5cmd ls s3://my-buck<tab> # This will complete this to "s3://my-bucket"

# These commands below will recommend up to 20 S3 objects or complete if there's only one match:
s5cmd ls s3://my-bucket/<tab><tab>
s5cmd ls s3://my-bucket/my-prefix/<tab><tab>
s5cmd ls s3://my-bucket/my-prefix/some-object<tab>
```

## Usage ##

```
$ ./s5cmd

Usage: s5cmd [OPTION]... [COMMAND [PARAMS...]]

Options:
  -cmp-install
    	Install shell completion
  -cmp-uninstall
    	Uninstall shell completion
  -ds int
    	Multipart chunk size in MB for downloads (default 50)
  -dw int
    	Download concurrency (single file) (default 5)
  -endpoint-url string
    	Override default URL with the given one
  -f string
    	Commands-file or - for stdin
  -gops
    	Initialize gops agent
  -install
    	Install completion for s5cmd command
  -no-verify-ssl
    	Don't verify SSL certificates
  -numworkers int
    	Number of worker goroutines. Negative numbers mean multiples of the CPU core count. (default 256)
  -r int
    	Retry S3 operations N times before failing (default 10)
  -stats
    	Always print stats
  -uninstall
    	Uninstall completion for s5cmd command
  -us int
    	Multipart chunk size in MB for uploads (default 50)
  -uw int
    	Upload concurrency (single file) (default 5)
  -version
    	Prints current version
  -vv
    	Verbose output
  -y	Don't prompt user for typing 'yes'

Commands:
    !, cp, du, exit, get, ls, mv, rm

To get help on a specific command, run "s5cmd <command> -h"
```

## Commands File ##

The most powerful feature of s5cmd is the commands file. Thousands of S3 and filesystem commands are declared in a file (or simply piped in from another process) and they are executed using multiple parallel workers. Since only one program is launched, thousands of unnecessary fork-exec calls are avoided. This way S3 execution times can reach a few thousand operations per second.

See also: [Nested Commands](#nested-commands-basic) 

## Single command invocation ##

Single commands are also supported with the `s5cmd [command [params]]` syntax.

## Supported commands ##

There are three main commands: `cp`, `mv` and `rm`. Arguments can be either S3 urls, S3 wildcards, local file/directory references or local glob patterns.

- Copy, Download or Upload: `cp [options] [src] [dst]`
    - Note, the directory hierarchy will be flattened by default. Pass `--parents` to preserve the dir structure
- Move, Download or Upload and then delete: `mv [options] [src] [dst]`
    - Note, the directory hierarchy will be flattened by default. Pass `--parents` to preserve the dir structure
- Delete: `rm [src]`
- Count objects and determine total size: `du [src]`
- Arbitrary shell-execute - `! commands...`
- Exit - `exit [exitcode]` (see [Exit Code](#exit-code))

### Command options ###
- S3 urls should be in the format `s3://bucket/key`
- `cp` and `mv` commands accept the `-n` (no-clobber) option to prevent overwriting existing files or objects, `-s` option to match source-destination file sizes and skip upload if sizes are equal, and `-u` option to match source-destination last modification times and skip upload if destination is newer or same. If these options are combined, overwrite is skipped only if all of the specified conditions are met.
- Uploading `cp` and `mv` commands accept the `-rr` and `-ia` options to store objects in reduced-redundancy and infrequent-access modes respectively.
- Batch `cp` and `mv` commands also accept the `--parents` option to create the dir structure in destination. Dir structure is created from the first wildcard onwards.
- Batch local-to-local `cp` and `mv` commands also accept the `-R` option for recursive operation.
- The `ls` command accepts the `-e` option to show ETags in listing.
- The `du` command only takes S3 arguments (prefix or wildcard)
- `ls` and `du` commands both accept the `-H` option to show human-readable object sizes.
- `du` command also accepts the `-g` option to group by storage class.


### Command examples ###

```
cp s3://from-bucket/from-key s3://to-bucket/[to-key] # Copy object in S3 
mv s3://from-bucket/from-key s3://to-bucket/[to-key] # Move object in S3
rm s3://del-bucket/del-key # Delete S3 object
cp /path/to/src/file /path/to/dest[/] # Copy local file
mv /path/to/src/file /path/to/dest[/] # Move local file
cp s3://from-bucket/from-key /path/to/dest[/] # Download from S3
rm /path/to/del # Delete local file or (empty) directory
ls # List buckets
ls s3://bucket[/prefix] # List objects in bucket
cp /path/to/src s3://to-bucket/to-key[/] # Upload to S3
cp /path/to/src/dir/ s3://to-bucket/to-prefix/ # Upload directory to S3
cp /path/to/src/*.go s3://to-bucket/to-prefix/ # Upload glob to S3
```

## Wild operations ##

Multiple-level wildcards are supported in S3 operations. This is achieved by listing all S3 objects with the prefix up to the first wildcard, then filtering the results in-memory. ie. For batch-downloads, first a `ls` call is made, the results are then converted to separate commands and executed in parallel. 

Batch API is used deleting multiple S3 objects, so up to 1000 S3 objects can be deleted with a single call.

### Wild operation examples ###
```
ls s3://bucket/prefix/*/file*gz # Wild-list objects in bucket
cp s3://from-bucket/prefix/*/file*gz /path/to/dest/ # Wild-download from S3
mv s3://from-bucket/prefix/*/file*gz /path/to/dest/ # Wild-download from S3, followed by delete
rm s3://from-bucket/prefix/*/file*gz # Wild-delete S3 objects (Batch-API)
```

### Tips ###

- Comments start with a space followed by `#`, as in " # This is a comment"
- Empty lines are also ok
- `-numworkers -1` means use `runtime.NumCPU` goroutines. `-2` means `2*runtime.NumCPU` and so on.
- The S3 throttling error `SlowDown` is exponentially retried. "Retryable operations" as specified by the AWS SDK (currently `RequestError` and `RequestError`) are retried by the SDK.

### Nested Commands (Basic) ###

Success and fail commands can be specified with `&&` and `||` operators. As the parser is pretty simple, multiple-level nested commands (doing something based on a result of a result) are not supported.

If you want to move an object between s3 buckets and then delete a local file if successful, you can do this:

```
mv s3://source-bkt/key s3://dest-bkt/key && rm /path/to/key
```

This is also valid:

```
mv a b/ || ! touch could-not-move # This is a comment
```

This as well:
```
! touch a && ! touch a-touched || ! touch a-couldnotbetouched
```

### S3 Credentials ###
S3 credentials can be provided in a variety of ways.

#### Full environment variables ####

Provide full S3 credentials with the environment variables:
```
AWS_ACCESS_KEY_ID=YOURKEY AWS_SECRET_ACCESS_KEY=ohnosecret AWS_REGION=us-east-1 ./s5cmd ls
```

#### Credentials file ####

Use the `$HOME/.aws/credentials` file:
```
[default]
aws_access_key_id = YOURKEY
aws_secret_access_key = ohnosecret
region = us-east-1
```
Then run s5cmd:
```
./s5cmd ls
```

To use a different profile, set the `AWS_PROFILE` env var. For more options, see the [AWS SDK Configuration](http://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html) page.

## Output ##

The general output is in the format:
```
DATE TIME Short-Msg Detailed-Msg
```

 - Trivial messages start with `#`, like number of workers or exit code and statistics
 - `+OK` for successful operations: `+OK "! touch touch-this-file"`
 - `-ERR` for failed operations: `-ERR "! touche": executable file not found in $PATH`
 - `?ErrorCode` for AWS-related errors, which will be retried (`?SlowDown`, `?InternalError`, etc)

Item output (used in `ls`) is slightly different: `DATE TIME` fields are omitted, and the short-msg is a single `+` character.

Shell output (used in `!`) does not modify the executed command's output. Both `stdout` and `stderr` are mirrored.

### Exit Code ###

If failed jobs are present, process exits with code `127`. This can be overridden with the command `exit`, though in that case finishing the job list is not guaranteed.

## Environment Variables ##

Set `S5CMD_GOPS` to always enable the [gops](https://github.com/google/gops) agent.


## Supported platforms ##

- s5cmd is tested on Linux and macOS. Should work on Windows, however not tested as of release time.
- Go 1.13 and up is supported.
- Use in production environments is OK. (it works fine for us -- but as always with trying out new tools, proceed with caution)
