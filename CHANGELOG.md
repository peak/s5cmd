# Changelog

## not released yet

This is a major release with many breaking changes.

#### Backwards incompatible changes

- Dropped `get` command. Users could get the same effect with `s5cmd cp <src> .`.
- Dropped `nested command` support.
- Dropped `!` command. It was used to execute shell commands and was used in
  conjunction with nested commands.
- `s5cmd -f` and `s5cmd -f -` usage has changed to `s5cmd run`. `run` command
  accepts a file. If not provided, it'll listen for commands from stdin.
- Exit code for errors was `127`. It is `1` now.
- Dropped `exit` command. It was used to change the shell exit code and usually
  a part of the nested command usage.
- All error messages are sent to stderr now.
- `-version` flag is changed to `version` command.
- Dropped `batch-rm` command. It was not listed in the help output. Now that we
  support variadic arguments, users can remove multiple objects by providing
  wildcards or multiple arguments to `s5cmd rm` command.
- [Virtual host style bucket name
  resolving](https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story/)
  is enabled by default for S3 and GCS. If you provide a custom endpoint via
  `--endpoint-url` flag (other than GCS and S3 transfer acceleration), `s5cmd`
  will fall back to the `path-style`. See [#92](https://github.com/peak/s5cmd/pull/92).
- Listing a non-existent object will return exit code `1`, instead of `0`. See [#23](https://github.com/peak/s5cmd/issues/23).

#### Features

- Added `mb` command to make buckets. See [#25](https://github.com/peak/s5cmd/issues/25).
- Added `--json` flag for JSON logging. See [#22](https://github.com/peak/s5cmd/issues/22).
- Added [S3 transfer acceleration](https://docs.aws.amazon.com/AmazonS3/latest/dev/transfer-acceleration.html) support. See [#40](https://github.com/peak/s5cmd/issues/40).
- Added varargs support. See [#2](https://github.com/peak/s5cmd/issues/2).

#### Bugfixes

- Correctly set `Content-Type` of a file on upload operations. See [#33](https://github.com/peak/s5cmd/issues/33).

#### Improvements

- Pre-compiled binaries are provided on [releases page](https://github.com/peak/s5cmd/releases). See [#21](https://github.com/peak/s5cmd/issues/21).
- AWS Go SDK is updated to support IAM role for service accounts. See [#32](https://github.com/peak/s5cmd/issues/32).
- `s5cmd` now creates destination directory if missing.

## v0.7.0 - 27 Jan 2020

- Use go modules.
- Update minimum required Go version to 1.13.

## v0.6.2 - 24 Jan 2020

- Fix bug in brew install.
- Update travis configuration.

## v0.6.1 - 9 Jan 2020

- Integrate Travis CI.
- Add option to disable SSL verification.
- Add endpoint url flag to support S3 compatible services.
- Use client's endpoint in GetSessionForBucket.
- Upgrade minimum required Go version to 1.7.

## v0.6.0 - 30 Mar 2018

- Use 50mb chunks by default.
- Add human-readable output option -H.
- Implement "command -h".

## v0.5.8 - 15 Mar 2018

- Refactor retryable error handling.
- Autodetect bucket region in command completion.
- Add HomeBrew formula.

## v0.5.7 - 16 Aug 2017

- Add -s and -u options to overwrite files if sizes differ or files are lastly modified.
- Use constructor for *JobArgument.

## v0.5.6 - 15 Jun 2017

- Add -dlw, -dlp and -ulw configuration options for worker pool.

## v0.5.5 - 29 May 2017
- Fix get/cp without 2nd param or exact destination filename.

## v0.5.4 - 23 May 2017

- Implement shell auto completion.
- Add context support for batch AWS requests.
- Implement "s5cmd get".
- Reduce idle-timer values.
- Add option -vv to log parser errors verbosely.
- Implement "du -g" to group by storage class.

## v0.5.3 - 9 Mar 2017

- Use Go bool type instead of aws.Bool on recoverer.

## v0.5.2 - 9 Mar 2017

- Make RequestError retryable.

## v0.5.1 - 8 Mar 2017

- Implement verbose output (-vv flag).
- Add godoc for error types.
