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

Usage of ./s5cmd:
  -cs int
    	Multipart chunk size in MB for uploads (default 5)
  -f string
        Commands-file or - for stdin (default stdin)
  -numworkers int
        Number of worker goroutines. Negative numbers mean multiples of runtime.NumCPU (default 256)
  -r int
        Retry S3 operations N times before failing (default 10)
  -version
        Prints current version
```

### S3 Credentials
Provide S3 credentials with the environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` and optionally `AWS_SESSION_TOKEN`.

### Supported commands

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
- Exit - `exit [exitcode]`

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

