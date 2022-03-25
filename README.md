[![Go Report](https://goreportcard.com/badge/github.com/peak/s5cmd)](https://goreportcard.com/report/github.com/peak/s5cmd) ![Github Actions Status](https://github.com/peak/s5cmd/workflows/CI/badge.svg)

# s5cmd

## Overview
`s5cmd` is a very fast S3 and local filesystem execution tool. It comes with support
for a multitude of operations including tab completion and wildcard support
for files, which can be very handy for your object storage workflow while working
with large number of files.

There are already other utilities to work with S3 and similar object storage
services, thus it is natural to wonder what `s5cmd` has to offer that others don't.

In short, *`s5cmd` offers a very fast speed.*
Thanks to [Joshua Robinson](https://github.com/joshuarobinson) for his
study and experimentation on `s5cmd;` to quote his medium [post](https://medium.com/@joshua_robinson/s5cmd-for-high-performance-object-storage-7071352cc09d):
> For uploads, s5cmd is 32x faster than s3cmd and 12x faster than aws-cli.
>For downloads, s5cmd can saturate a 40Gbps link (~4.3 GB/s), whereas s3cmd
>and aws-cli can only reach 85 MB/s and 375 MB/s respectively.

If you would like to know more about performance of `s5cmd` and the
reasons for its fast speed, refer to [benchmarks](./README.md#Benchmarks) section
## Features
![](./doc/usage.png)

`s5cmd` supports wide range of object management tasks both for cloud
storage services and local filesystems.

- List buckets and objects
- Upload, download or delete objects
- Move, copy or rename objects
- Set Server Side Encryption using AWS Key Management Service (KMS)
- Set Access Control List (ACL) for objects/files on the upload, copy, move.
- Print object contents to stdout
- Select JSON records from objects using SQL expressions
- Create or remove buckets
- Summarize objects sizes, grouping by storage class
- Wildcard support for all operations
- Multiple arguments support for delete operation
- Command file support to run commands in batches at very high execution speeds
- Dry run support
- [S3 Transfer Acceleration](https://docs.aws.amazon.com/AmazonS3/latest/dev/transfer-acceleration.html) support
- Google Cloud Storage (and any other S3 API compatible service) support
- Structured logging for querying command outputs
- Shell auto-completion
- S3 ListObjects API backward compatibility

## Installation

### Binaries

The [Releases](https://github.com/peak/s5cmd/releases) page provides pre-built
binaries for Linux, macOS and Windows.

### Homebrew

For macOS, a [homebrew](https://brew.sh) tap is provided:

    brew tap peak/s5cmd https://github.com/peak/s5cmd
    brew install s5cmd

### MacPorts

You can also install `s5cmd` from [MacPorts](https://ports.macports.org/port/s5cmd/summary) on macOS:

    sudo port selfupdate
    sudo port install s5cmd

NOTE: MacPorts is not officially supported. The versions might be out of date compared to Homebrew.

### Build from source

You can build `s5cmd` from source if you have [Go](https://golang.org/dl/) 1.14+
installed.

    go get github.com/peak/s5cmd

⚠️ Please note that building from `master` is not guaranteed to be stable since
development happens on `master` branch.

### Docker

#### Hub
    $ docker pull peakcom/s5cmd
    $ docker run --rm -v ~/.aws:/root/.aws peakcom/s5cmd <S3 operation>

#### Build
    $ git clone https://github.com/peak/s5cmd && cd s5cmd
    $ docker build -t s5cmd .
    $ docker run --rm -v ~/.aws:/root/.aws s5cmd <S3 operation>

## Usage

`s5cmd` supports multiple-level wildcards for all S3 operations. This is
achieved by listing all S3 objects with the prefix up to the first wildcard,
then filtering the results in-memory. For example, for the following command;

    s5cmd cp 's3://bucket/logs/2020/03/*' .

first a `ListObjects` request is send, then the copy operation will be executed
against each matching object, in parallel.

### Examples

#### Download a single S3 object

    s5cmd cp s3://bucket/object.gz .

#### Download multiple S3 objects

Suppose we have the following objects:
```
s3://bucket/logs/2020/03/18/file1.gz
s3://bucket/logs/2020/03/19/file2.gz
s3://bucket/logs/2020/03/19/originals/file3.gz
```

    s5cmd cp 's3://bucket/logs/2020/03/*' logs/


`s5cmd` will match the given wildcards and arguments by doing an efficient
search against the given prefixes. All matching objects will be downloaded in
parallel. `s5cmd` will create the destination directory if it is missing.

`logs/` directory content will look like:

```
$ tree
.
└── logs
    ├── 18
    │   └── file1.gz
    └── 19
        ├── file2.gz
        └── originals
            └── file3.gz

4 directories, 3 files
```

ℹ️ `s5cmd` preserves the source directory structure by default. If you want to
flatten the source directory structure, use the `--flatten` flag.

    s5cmd cp --flatten 's3://bucket/logs/2020/03/*' logs/

`logs/` directory content will look like:

```
$ tree
.
└── logs
    ├── file1.gz
    ├── file2.gz
    └── file3.gz

1 directory, 3 files
```

#### Upload a file to S3

    s5cmd cp object.gz s3://bucket/

 by setting server side encryption (*aws kms*) of the file:

    s5cmd cp -sse aws:kms -sse-kms-key-id <your-kms-key-id> object.gz s3://bucket/

 by setting Access Control List (*acl*) policy of the object:

    s5cmd cp -acl bucket-owner-full-control object.gz s3://bucket/

#### Upload multiple files to S3

    s5cmd cp directory/ s3://bucket/

Will upload all files at given directory to S3 while keeping the folder hierarchy
of the source.

#### Delete an S3 object

    s5cmd rm s3://bucket/logs/2020/03/18/file1.gz

#### Delete multiple S3 objects

    s5cmd rm s3://bucket/logs/2020/03/19/*

Will remove all matching objects:

```
s3://bucket/logs/2020/03/19/file2.gz
s3://bucket/logs/2020/03/19/originals/file3.gz
```

`s5cmd` utilizes S3 delete batch API. If matching objects are up to 1000,
they'll be deleted in a single request. However, it should be noted that commands such as

    s5cmd rm s3://bucket-foo/object s3://bucket-bar/object

are not supported by `s5cmd` and result in error (since we have 2 different buckets), as it is in odds with the benefit of performing batch delete requests. Thus, if in need, one can use `s5cmd run` mode for this case, i.e,

    $ s5cmd run
    rm s3://bucket-foo/object
    rm s3://bucket-bar/object

more details and examples on `s5cmd run` are presented in a [later section](./README.md#L224).

#### Copy objects from S3 to S3

`s5cmd` supports copying objects on the server side as well.

    s5cmd cp 's3://bucket/logs/2020/*' s3://bucket/logs/backup/

Will copy all the matching objects to the given S3 prefix, respecting the source
folder hierarchy.

⚠️ Copying objects (from S3 to S3) larger than 5GB is not supported yet. We have
an [open ticket](https://github.com/peak/s5cmd/issues/29) to track the issue.

#### Select JSON object content using SQL

`s5cmd` supports the `SelectObjectContent` S3 operation, and will run your
[SQL query](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-glacier-select-sql-reference.html)
against objects matching normal wildcard syntax and emit matching JSON records via stdout. Records
from multiple objects will be interleaved, and order of the records is not guaranteed (though it's
likely that the records from a single object will arrive in-order, even if interleaved with other
records).

    $ s5cmd select --compression GZIP \
      --query "SELECT s.timestamp, s.hostname FROM S3Object s WHERE s.ip_address LIKE '10.%' OR s.application='unprivileged'" \
      s3://bucket-foo/object/2021/*
    {"timestamp":"2021-07-08T18:24:06.665Z","hostname":"application.internal"}
    {"timestamp":"2021-07-08T18:24:16.095Z","hostname":"api.github.com"}

At the moment this operation _only_ supports JSON records selected with SQL. S3 calls this
lines-type JSON, but it seems that it works even if the records aren't line-delineated. YMMV.

#### Count objects and determine total size

    $ s5cmd du --humanize 's3://bucket/2020/*'

    30.8M bytes in 3 objects: s3://bucket/2020/*

#### Run multiple commands in parallel

The most powerful feature of `s5cmd` is the commands file. Thousands of S3 and
filesystem commands are declared in a file (or simply piped in from another
process) and they are executed using multiple parallel workers. Since only one
program is launched, thousands of unnecessary fork-exec calls are avoided. This
way S3 execution times can reach a few thousand operations per second.

    s5cmd run commands.txt

or

    cat commands.txt | s5cmd run

`commands.txt` content could look like:

```
cp s3://bucket/2020/03/* logs/2020/03/

# line comments are supported
rm s3://bucket/2020/03/19/file2.gz

# empty lines are OK too like above

# rename an S3 object
mv s3://bucket/2020/03/18/file1.gz s3://bucket/2020/03/18/original/file.gz

# list all buckets
ls # inline comments are OK too
```

#### Sync
`sync` command synchronizes S3 buckets, prefixes, directories and files between S3 buckets and prefixes as well.
It compares files between source and destination, taking source files as **source-of-truth**;

* copies files those do not exist in destination
* copies files those exist in both locations if the comparison made with sync strategy allows it so

It makes a one way synchronization from source to destination without modifying any of the source files and deleting any of the destination files (unless `--delete` flag has passed).

Suppose we have following files;
```
   -  29 Sep 10:00 .
5000  29 Sep 11:00 ├── favicon.ico
 300  29 Sep 10:00 ├── index.html
  50  29 Sep 10:00 ├── readme.md
  80  29 Sep 11:30 └── styles.css
```

```
s5cmd ls s3://bucket/static/
2021/09/29 10:00:01               300 index.html
2021/09/29 11:10:01                10 readme.md
2021/09/29 10:00:01                90 styles.css
2021/09/29 11:10:01                10 test.html
```
running would;
* copy `favicon.ico`
  * file does not exist in destination.
* copy `styles.css`
  * source file is newer than to remote counterpart.
* copy `readme.md`
  * even though the source one is older, it's size differs from the destination one; assuming source file is the source of truth.
```
s5cmd sync . s3://bucket/static/

cp favicon.ico s3://bucket/static/favicon.ico
cp styles.css s3://bucket/static/styles.css
cp readme.md s3://bucket/static/readme.md
```

Running with `--delete` flag would delete files those do not exist in the source;
```
s5cmd sync --delete . s3://bucket/static/

rm s3://bucket/test.html
cp favicon.ico s3://bucket/static/favicon.ico
cp styles.css s3://bucket/static/styles.css
cp readme.md s3://bucket/static/readme.md
```

It's also possible to use wildcards to sync only a subset of files.

To sync only `.html` files in S3 bucket above to same local file system;

```
s5cmd sync 's3://bucket/static/*.html' .

cp s3://bucket/prefix/index.html index.html
cp s3://bucket/prefix/test.html test.html
```

##### Strategy
###### Default
By default `s5cmd` compares files' both size **and** modification times, treating source files as **source of truth**. Any difference in size or modification time would cause `s5cmd` to copy source object to destination.

mod time    |  size        |  should sync
------------|--------------|-------------
src > dst   |  src != dst  |  ✅
src > dst   |  src == dst  |  ✅
src <= dst  |  src != dst  |  ✅
src <= dst  |  src == dst  |  ❌

###### Size only
With `--size-only` flag, it's possible to use the strategy that would only compare file sizes. Source treated as **source of truth** and any difference in sizes would cause `s5cmd` to copy source object to destination.

mod time   |  size        |  should sync
-----------|--------------|-------------
src > dst  |  src != dst  |  ✅
src > dst  |  src = dst   |  ❌
src <= dst  |  src != dst  |  ✅
src <= dst  |  src == dst  |  ❌

### Dry run
`--dry-run` flag will output what operations will be performed without actually
carrying out those operations.

    s3://bucket/pre/file1.gz
    ...
    s3://bucket/last.txt

running

    s5cmd --dry-run cp s3://bucket/pre/* s3://another-bucket/

will output

    cp s3://bucket/pre/file1.gz s3://another-bucket/file1.gz
    ...
    cp s3://bucket/pre/last.txt s3://anohter-bucket/last.txt

however, those copy operations will not be performed. It is displaying what
`s5cmd` will do when ran without `--dry-run`

Note that `--dry-run` can be used with any operation that has a side effect, i.e.,
cp, mv, rm, mb ...

### S3 ListObjects API Backward Compatibility

The `--use-v1-api` flag will force using S3 ListObjects API instead of ListObjectsV2 API.

```
s5cmd --use-v1-api ls
```

### Specifying credentials

`s5cmd` uses official AWS SDK to access S3. SDK requires credentials to sign
requests to AWS. Credentials can be provided in a variety of ways:

- Environment variables
- AWS credentials file, including profile selection via `AWS_PROFILE` environment
  variable
- If `s5cmd` runs on an Amazon EC2 instance, EC2 IAM role
- If `s5cmd` runs on EKS, Kube IAM role

The SDK detects and uses the built-in providers automatically, without requiring
manual configurations.

### Region detection

While executing the commands, `s5cmd` detects the region according to the following order of priority:

1. `--source-region` or `--destination-region` flags of `cp` command.
2. `AWS_REGION` environment variable.
3. Region section of AWS profile.
4. Auto detection from bucket region (via `HeadBucket`).
5. `us-east-1` as default region.


### Shell auto-completion

Shell completion is supported for bash, zsh and fish.

To enable auto-completion, run:

    s5cmd --install-completion

This will add a few lines to your shell configuration file. After installation,
restart your shell to activate the changes.

### Google Cloud Storage support

`s5cmd` supports S3 API compatible services, such as GCS, Minio or your favorite
object storage.

    s5cmd --endpoint-url https://storage.googleapis.com ls

or an alternative with environment variable

    S3_ENDPOINT_URL="https://storage.googleapis.com" s5cmd ls

    # or

    export S3_ENDPOINT_URL="https://storage.googleapis.com"
    s5cmd ls

all variants will return your GCS buckets.

`s5cmd` will use virtual-host style bucket resolving for S3, S3 transfer
acceleration and GCS. If a custom endpoint is provided, it'll fallback to
path-style.

### Retry logic

`s5cmd` uses an exponential backoff retry mechanism for transient or potential
server-side throttling errors. Non-retriable errors, such as `invalid
credentials`, `authorization errors` etc, will not be retried. By default,
`s5cmd` will retry 10 times for up to a minute. Number of retries are adjustable
via `--retry-count` flag.

ℹ️ Enable debug level logging for displaying retryable errors.

## Using wildcards

Most shells can attempt to expand wildcards before passing the arguments to
`s5cmd`, resulting in surprising `no matches found` errors.

To avoid this problem, surround the wildcarded expression with single quotes.

## Output

`s5cmd` supports both structured and unstructured outputs.
* unstructured output

```shell
$ s5cmd cp s3://bucket/testfile .

cp s3://bucket/testfile testfile
```

```shell
$ s5cmd cp --no-clobber s3://somebucket/file.txt file.txt

ERROR "cp s3://somebucket/file.txt file.txt": object already exists
```

* If `--json` flag is provided:

```json
{
    "operation": "cp",
    "success": true,
    "source": "s3://bucket/testfile",
    "destination": "testfile",
    "object": "[object]"
}
{
    "operation": "cp",
    "job": "cp s3://somebucket/file.txt file.txt",
    "error": "'cp s3://somebucket/file.txt file.txt': object already exists"
}
```
## Benchmarks
Some benchmarks regarding the performance of `s5cmd` are introduced below. For more
details refer to this [post](https://medium.com/@joshua_robinson/s5cmd-for-high-performance-object-storage-7071352cc09d)
which is the source of the benchmarks to be presented.

*Upload/download of single large file*

<img src="./doc/benchmark1.png" alt="get/put performance graph" height="75%" width="75%">

*Uploading large number of small-sized files*

<img src="./doc/benchmark2.png" alt="multi-object upload performance graph" height="75%" width="75%">

*Performance comparison on different hardware*

<img src="./doc/benchmark3.png" alt="s3 upload speed graph" height="75%" width="75%">

*So, where does all this speed come from?*

There are mainly two reasons for this:
- It is written in Go, a statically compiled language designed to make development
of concurrent systems easy and make full utilization of multi-core processors.
- *Parallelization.* `s5cmd` starts out with concurrent worker pools and parallelizes
workloads as much as possible while trying to achieve maximum throughput.

# Advanced Usage

Some of the advanced usage patterns provided below are inspired by the following [article](https://medium.com/@joshua_robinson/s5cmd-hits-v1-0-and-intro-to-advanced-usage-37ad02f7e895) (thank you! [@joshuarobinson](https://github.com/joshuarobinson))

## Integrate s5cmd operations with Unix commands
Assume we have a set of objects on S3, and we would like to list them in sorted fashion according to object names.

    $ s5cmd ls s3://bucket/reports/ | sort -k 4
    2020/08/17 09:34:33              1364 antalya.csv
    2020/08/17 09:34:33                 0 batman.csv
    2020/08/17 09:34:33             23114 istanbul.csv
    2020/08/17 09:34:33             26154 izmir.csv
    2020/08/17 09:34:33               112 samsun.csv
    2020/08/17 09:34:33             12552 van.csv

For a more practical scenario, let's say we have an [avocado prices](https://www.kaggle.com/neuromusic/avocado-prices) dataset, and we would like to take a peek at the few lines of the data by fetching only the necessary bytes.

    $ s5cmd cat s3://bucket/avocado.csv.gz | gunzip | xsv slice --len 5 | xsv table
        Date        AveragePrice  Total Volume  4046     4225       4770   Total Bags  Small Bags  Large Bags  XLarge Bags  type          year  region
    0   2015-12-27  1.33          64236.62      1036.74  54454.85   48.16  8696.87     8603.62     93.25       0.0          conventional  2015  Albany
    1   2015-12-20  1.35          54876.98      674.28   44638.81   58.33  9505.56     9408.07     97.49       0.0          conventional  2015  Albany
    2   2015-12-13  0.93          118220.22     794.7    109149.67  130.5  8145.35     8042.21     103.14      0.0          conventional  2015  Albany
    3   2015-12-06  1.08          78992.15      1132.0   71976.41   72.58  5811.16     5677.4      133.76      0.0          conventional  2015  Albany
    4   2015-11-29  1.28          51039.6       941.48   43838.39   75.78  6183.95     5986.26     197.69      0.0          conventional  2015  Albany


## Beast Mode s5cmd

`s5cmd` allows to pass in some file, containing list of operations to be performed, as an argument to the `run` command as illustrated in the [above](./README.md#L224) example. Alternatively, one can pipe in commands into
the `run:`

    BUCKET=s5cmd-test; s5cmd ls s3://$BUCKET/*test | grep -v DIR | awk ‘{print $NF}’
    | xargs -I {} echo “cp s3://$BUCKET/{} /local/directory/” | s5cmd run

The above command performs two `s5cmd` invocations; first, searches for files with *test* suffix and then creates a *copy to local directory* command for each matching file and finally, pipes in those into the ` run.`

Let's examine another usage instance, where we migrate files older than
30 days to a cloud object storage:

    find /mnt/joshua/nachos/ -type f -mtime +30 | awk '{print "mv "$1" s3://joshuarobinson/backup/"$1}'
    | s5cmd run

It is worth to mention that, `run` command should not be considered as a *silver bullet* for all operations. For example, assume we want to remove the following objects:

    s3://bucket/prefix/2020/03/object1.gz
    s3://bucket/prefix/2020/04/object1.gz
    ...
    s3://bucket/prefix/2020/09/object77.gz

Rather than executing

    rm s3://bucket/prefix/2020/03/object1.gz
    rm s3://bucket/prefix/2020/04/object1.gz
    ...
    rm s3://bucket/prefix/2020/09/object77.gz

with `run` command, it is better to just use

    rm s3://bucket/prefix/2020/0*/object*.gz

the latter sends single delete request per thousand objects, whereas using the former approach
sends a separate delete request for each subcommand provided to `run.` Thus, there can be a
significant runtime difference between those two approaches.


# LICENSE

MIT. See [LICENSE](https://github.com/peak/s5cmd/blob/master/LICENSE).
