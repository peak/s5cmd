# Changelog

## not released yet


## v1.2.1 - 3 Dec 2020

#### Improvements

- Statically link `s5cmd` in Docker image ([#250](https://github.com/peak/s5cmd/issues/250))

#### Bugfixes

- Fixed a bug where HeadBucket request fails during region information retrieval. ([#251](https://github.com/peak/s5cmd/issues/251), [#252](https://github.com/peak/s5cmd/issues/252))


## v1.2.0 - 5 Nov 2020

With this release, `s5cmd` automatically determines region information of destination buckets.

#### Features

- Added global `--dry-run` option. It displays which command(s) will be executed without actually having a side effect. ([#90](https://github.com/peak/s5cmd/issues/90))
- Added `--stat` option for `s5cmd` and it displays program execution statistics before the end of the program output. ([#148](https://github.com/peak/s5cmd/issues/148))
- Added cross-region transfer support. Bucket regions are inferred, thus, supporting cross-region transfers and multiple regions in batch mode. ([#155](https://github.com/peak/s5cmd/issues/155))

#### Bugfixes

- Fixed incorrect MIME type inference for `cp`, give priority to file extension for type inference. ([#214](https://github.com/peak/s5cmd/issues/214))
- Fixed error reporting issue, where some errors from the `ls` operation were not printed.

#### Improvements

- Requests to different buckets not allowed in `rm` batch operation, i.e., throw an error.
- AWS S3 `RequestTimeTooSkewed` request error was not retryable before, it is now. ([205](https://github.com/peak/s5cmd/issues/205))
- For some operations errors were printed at the end of the program execution. Now, errors are displayed immediately after being detected. ([#136](https://github.com/peak/s5cmd/issues/136))
- From now on, docker images will be published on Docker Hub. ([#238](https://github.com/peak/s5cmd/issues/238))
- Changed misleading 'mirroring' examples in the help text of `cp`. ([#213](https://github.com/peak/s5cmd/issues/213))


## v1.1.0 - 22 Jul 2020

With this release, Windows is supported.

#### Breaking changes

- Dropped storage class short codes display from default behaviour of `ls` operation. Instead, use `-s` flag with `ls`
to see full names of the storage classes when listing objects.


#### Features

- Added Server-side Encryption (SSE) support for mv/cp operations. It uses customer master keys (CMKs) managed by AWS Key Management Service. ([#18](https://github.com/peak/s5cmd/issues/18))
- Added an option to show full form of [storage class](https://aws.amazon.com/s3/storage-classes/) when listing objects. ([#165](https://github.com/peak/s5cmd/issues/165))
- Add [access control lists (ACLs)](https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html)
support to enable managing access to buckets and objects. ([#26](https://github.com/peak/s5cmd/issues/26))


#### Bugfixes

- Fixed infinite repetition issue on mv/cp operations which would occur
 if the destination matched the source wildcard. ([#168](https://github.com/peak/s5cmd/issues/168))
- Fixed windows filepath issue, where backslashes should be treated as the path delimiter. ([#178](https://github.com/peak/s5cmd/issues/178))
- All tests pass on windows, by converting and treating file paths to UNIX filepath format.
- Fixed a transfer issue where the object path contains particular regex metacharacters. ([#111](https://github.com/peak/s5cmd/pull/111)) [@brendan-matroid](https://github.com/brendan-matroid)
- Correctly parse object paths that contain whitespaces in run-mode. ([#111](https://github.com/peak/s5cmd/pull/111)) [@brendan-matroid](https://github.com/brendan-matroid)


#### Improvements

- Retry when connection closed by S3 unexpectedly. ([#189](https://github.com/peak/s5cmd/pull/189)) [@eminugurkenar](https://github.com/eminugurkenar)

## v1.0.0 - 1 Apr 2020

This is a major release with many breaking changes.

#### Breaking changes

- Dropped `get` command. Users could get the same effect with `s5cmd cp <src> .`.
- Dropped `nested command` support.
- Dropped `!` command. It was used to execute shell commands and was used in
  conjunction with nested commands.
- `s5cmd -f` and `s5cmd -f -` usage has changed to `s5cmd run`. `run` command
  accepts a file. If not provided, it'll listen for commands from stdin.
- Exit code for errors was `127`. It is `1` now.
- Dropped `exit` command. It was used to change the shell exit code and usually
  a part of the nested command usage.
- Dropped local->local copy and move support. ([#118](https://github.com/peak/s5cmd/issues/118))
- All error messages are sent to stderr now.
- `-version` flag is changed to `version` command.
- Dropped `batch-rm` command. It was not listed in the help output. Now that we
  support variadic arguments, users can remove multiple objects by providing
  wildcards or multiple arguments to `s5cmd rm` command. ([#106](https://github.com/peak/s5cmd/pull/106))
- [Virtual host style bucket name
  resolving](https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story/)
  is enabled by default for S3 and GCS. If you provide a custom endpoint via
  `--endpoint-url` flag (other than GCS and S3 transfer acceleration), `s5cmd`
  will fall back to the `path-style`. ([#92](https://github.com/peak/s5cmd/pull/92))
- Listing a non-existent object will return exit code `1`, instead of `0`. ([#23](https://github.com/peak/s5cmd/issues/23))
- `-ds`, `-dw`, `-us` and `-uw` global flags are no longer available. Multipart
  concurrency and part size flags are now part of the `cp/mv` command. New
  replacement flags are `--concurrency | -c` and `--part-size | -p`. ([#110](https://github.com/peak/s5cmd/pull/110))
- s5cmd `cp` command follows symbolic links by default (only when uploading to
  s3 from local filesystem). Use `--no-follow-symlinks` flag to disable this
  feature. ([#17](https://github.com/peak/s5cmd/issues/17))
- Dropped `-parents` flag from copy command. Copy behaviour has changed to
  preserve the directory hierarchy as a default. Optional `-flatten` flag is
  added to flatten directory structure. ([#107](https://github.com/peak/s5cmd/issues/107))
- Dropped `-vv` verbosity flag. `--log` flag is introduced.

#### Features

- Added `mb` command to make buckets. ([#25](https://github.com/peak/s5cmd/issues/25))
- Added `--json` flag for JSON logging. ([#22](https://github.com/peak/s5cmd/issues/22))
- Added [S3 transfer acceleration](https://docs.aws.amazon.com/AmazonS3/latest/dev/transfer-acceleration.html) support. ([#40](https://github.com/peak/s5cmd/issues/40))
- Added [Google Cloud Storage](https://github.com/peak/s5cmd#google-cloud-storage-support) support. ([#81](https://github.com/peak/s5cmd/issues/81))
- Added `cat` command to print remote object contents to stdout ([#20](https://github.com/peak/s5cmd/issues/20))

#### Bugfixes

- Correctly set `Content-Type` of a file on upload operations. ([#33](https://github.com/peak/s5cmd/issues/33))
- Fixed a bug where workers are unable to consume job if there are too many
  outstanding wildcard expansion requests. ([#12](https://github.com/peak/s5cmd/issues/12), [#58](https://github.com/peak/s5cmd/issues/58))

#### Improvements

- Pre-compiled binaries are provided on [releases page](https://github.com/peak/s5cmd/releases). ([#21](https://github.com/peak/s5cmd/issues/21))
- AWS Go SDK is updated to support IAM role for service accounts. ([#32](https://github.com/peak/s5cmd/issues/32))
- For copy/move operations, `s5cmd` now creates destination directory if missing.
- Increase the soft limit of open files to 1000 and exits immediately when it encounters `too many open files` error. ([#52](https://github.com/peak/s5cmd/issues/52))

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
