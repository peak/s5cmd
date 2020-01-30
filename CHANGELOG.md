# Changelog

## v0.7.0

- Use go modules.
- Update minimum required Go version to 1.13.

## v0.6.2

- Fix bug in brew install.
- Update travis configuration.

## v0.6.1

- Integrate Travis CI.
- Add option to disable SSL verification.
- Add endpoint url flag to support S3 compatible services.
- Use client's endpoint in GetSessionForBucket.
- Upgrade minimum required Go version to 1.7.

## v0.6.0

- Use 50mb chunks by default.
- Add human-readable output option -H. 
- Implement "command -h".

## v0.5.8

- Refactor retryable error handling.
- Autodetect bucket region in command completion.
- Add HomeBrew formula.

## v0.5.7

- Add -s and -u options to overwrite files if sizes differ or files are lastly modified.
- Use constructor for *JobArgument.

## v0.5.6

- Add -dlw, -dlp and -ulw configuration options for worker pool.

## v0.5.5
- Fix get/cp without 2nd param or exact destination filename.

## v0.5.4

- Implement shell auto completion.
- Add context support for batch AWS requests.
- Implement "s5cmd get".
- Reduce idle-timer values.
- Add option -vv to log parser errors verbosely.
- Implement "du -g" to group by storage class.

## v0.5.3
- Use Go bool type instead of aws.Bool on recoverer.

## v0.5.2

- Make RequestError retryable.

## v0.5.1

- Implement verbose output (-vv flag).
- Add godoc for error types.