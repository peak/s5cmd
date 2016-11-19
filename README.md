# s5cmd

This is a parallel S3 and local filesystem execution tool.

### Build

Execute:

```bash
$ make
```
in the root directory and you'll get a binary named `s5cmd`.

## Usage

```bash
$ ./s5cmd --help

Usage: ./s5cmd [OPTION]... [COMMAND [PARAMS...]]
 
  -cs int
    	Multipart chunk size in MB for uploads (default 5)
  -f string
        Commands-file or - for stdin
  -numworkers int
        Number of worker goroutines. Negative numbers mean multiples of runtime.NumCPU (default 256)
  -r int
        Retry S3 operations N times before failing (default 10)
  -stats
        Always print stats
  -version
        Prints current version
```

## Commands File

The most powerful feature of s5cmd is the commands file. Thousands of S3 and filesystem commands are declared in a file (or simply piped in from another process) and they are executed using multiple parallel workers. Since only one program is launched, thousands of unnecessary fork-exec calls are avoided. This way S3 execution times can reach a few thousand operations per second.

See also: [Nested Commands](#nested-commands-basic) 

## Single command invocation

Single commands are also supported with the `s5cmd [command [params]]` syntax. If this syntax is used, only one worker is launched.

## Supported commands

S3 urls should be in the format `s3://bucket/key`

- Copy object in S3 - `cp s3://from-bucket/from-key s3://to-bucket/[to-key]`
- Move object in S3 - `mv s3://from-bucket/from-key s3://to-bucket/[to-key]`
- Delete S3 object  - `rm s3://del-bucket/del-key`
- Copy local file - `!cp /path/to/src/file /path/to/dest[/]`
- Move local file - `!mv /path/to/src/file /path/to/dest[/]`
- Delete local file or (empty) directory - `!rm /path/to/del`
- Arbitrary shell-execute - `! commands...`
- Download from S3 - `get s3://from-bucket/from-key [/path/to/dest[/]]`
- Upload to S3 - `put /path/to/src s3://to-bucket/to-key[/]`
- List buckets - `ls`
- List objects in bucket - `ls s3://bucket[/prefix]`
- List objects filtered by multiple-level wildcards - `ls s3://bucket/prefix/*/file*gz`
- Exit - `exit [exitcode]` (see [Exit Code](#exit-code))

### Tips

- `! cp` and `!cp` are two different commands, the latter does the copying in Go, the former probably executes `/bin/cp` 
- Comments start with a space followed by `#`, as in " # This is a comment"
- Empty lines are also ok
- `-numworkers -1` means use `runtime.NumCPU` goroutines. `-2` means `2*runtime.NumCPU` and so on.
- The S3 throttling error `SlowDown` is exponentially retried. "Retryable operations" as specified by the AWS SDK (currently `RequestError` and `RequestError`) are retried by the SDK.

### Nested Commands (Basic)

Success and fail commands can be specified with `&&` and `||` operators. As the parser is pretty simple, multiple-level nested commands (doing something based on a result of a result) are not supported.

If you want to move an object between s3 buckets and then delete a local file if successful, you can do this:

```
mv s3://source-bkt/key s3://dest-bkt/key && !rm /path/to/key
```

This is also valid:

```
!mv a b/ || ! touch could-not-move # This is a comment
```

This as well:
```
! touch a && ! touch a-touched || ! touch a-couldnotbetouched
```

### S3 Credentials
Provide S3 credentials with the environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` and optionally `AWS_SESSION_TOKEN`.

## Output

The general output is in the format:
```
DATE TIME Short-Msg Detailed-Msg
```

 - Trivial messages start with `#`, like number of workers or exit code and statistics
 - `+OK` for successful operations: `+OK "! touch touch-this-file"`
 - `-ERR` for failed operations: `-ERR "! touche": executable file not found in $PATH`
 - `?Ratelimit` for rate-limited operations, which will be retried

Item output (used in `ls`) is slightly different: `DATE TIME` fields are omitted, and the short-msg is a single `+` character.

Shell output (used in `!`) does not modify the executed command's output. Both `stdout` and `stderr` are mirrored.

### Exit Code

If failed jobs are present, process exits with code `127`. This can be overridden with the command `exit`, though in that case finishing the job list is not guaranteed.
