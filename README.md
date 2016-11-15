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
  -f string
        Commands-file or - for stdin (default "-")
  -numworkers int
        Number of worker goroutines. (default runtime.NumCPU)
```

### S3 Credentials
Provide S3 credentials with the environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` and optionally `AWS_SESSION_TOKEN`.

### Supported commands

S3 urls should be in the format `s3://bucket/key`

- Copy object in S3 - `cp s3://from-bucket/from-key s3://to-bucket/to-key`
- Move object in S3 - `mv s3://from-bucket/from-key s3://to-bucket/to-key`
- Delete S3 object  - `del s3://del-bucket/del-key`
- Copy local file - `local-cp /path/to/src/file /path/to/dest[/]`
- Move local file - `local-mv /path/to/src/file /path/to/dest[/]`
- Delete local file or (empty) directory - `local-rm /path/to/del`
- Arbitrary shell-execute - `exec commands...`
- TODO - Download from S3 - `get s3://from-bucket/from-key /path/to/dest[/]`
- TODO - Upload to S3 - `put /path/to/src s3://to-bucket/to-key[/]`

