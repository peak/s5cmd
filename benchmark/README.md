
## s5cmd performance regression tests

`bench.py` allow us to compare two different build (from either version tag, PR number or commit tag) performance under various scenarios. These scenarios include:

1. Upload, Download, Remove many small sized file
1. Upload, Download, Remove large file
1. Upload, Download, Remove very large file

> To change the scenarios, you should edit it inside the `bench.py` for now. In the future, this could be read from a file. From each scenario, user should not forget to change the file size and file count keeping in mind the restrictions of their system.


### required tools
This script is dependent on the following tools. Make sure you install them to your system before running `bench.py`.
- git
- go 
- hyperfine
- truncate



To run use the following syntax:
```
usage: bench.py [-h] [-s OLD NEW] [-w WARMUP] [-r RUNS] [-o OUTPUT_FILE_NAME] -b BUCKET [-l LOCAL_PATH] [-p PREFIX] [-hf HYPERFINE_EXTRA_FLAGS] [-sf S5CMD_EXTRA_FLAGS]

Compare performance of two different builds of s5cmd.

optional arguments:
  -h, --help            show this help message and exit
  -s OLD NEW, --s5cmd OLD NEW
                        Reference to old and new s5cmd.It can be a decimal indicating PR number,any of the version tags like v2.0.0 or any commit tag. Additionally it can be 'latest_release' or
                        'master'. (default: ('latest_release', 'master'))
  -w WARMUP, --warmup WARMUP
                        Number of program executions before the actual benchmark: (default: 2)
  -r RUNS, --runs RUNS  Number of runs to perform for each command (default: 10)
  -o OUTPUT_FILE_NAME, --output_file_name OUTPUT_FILE_NAME
                        Name of the output file (default: summary.md)
  -b BUCKET, --bucket BUCKET
                        Name of the bucket in remote (default: None)
  -l LOCAL_PATH, --local-path LOCAL_PATH
                        specify a local path for temporary files to be loaded. (default: None)
  -p PREFIX, --prefix PREFIX
                        Key prefix to be used while uploading to a specified bucket (default: s5cmd-benchmarks-)
  -hf HYPERFINE_EXTRA_FLAGS, --hyperfine-extra-flags HYPERFINE_EXTRA_FLAGS
                        hyperfine global extra flags. Write in between quotation marks and start with a space to avoid bugs. (default: None)
  -sf S5CMD_EXTRA_FLAGS, --s5cmd-extra-flags S5CMD_EXTRA_FLAGS
                        s5cmd global extra flags. Write in between quotation marks and start with a space to avoid bugs. (default: None)
```

### Examples
```
./bench.py --bucket tempbucket  
```
Above command will compare `latest-release` to `master` with 2 warmup runs and 10 benchmark runs.  

```
./bench.py --bucket tempbucket --s5cmd v2.0.0 456 --warmup 2 --runs 10 
```
Above command will compare v2.0.0 to PR:456 with 2 warmup runs and 10 benchmark runs. 

```
./bench.py --bucket tempbucket --s5cmd v2.0.0 456 --warmup 2 --runs 10 -sf " --log error" -hf " --show-output"
```
When using `-hf` and `-sf` flags, use quotes like above and start with an empty space. If not started with an empty space, it might give an error. This is a known issue with `argparse` and [this](https://stackoverflow.com/questions/72129874/processing-arguments-for-subprocesses-using-argparse-expected-one-argument) discussion can be useful to understand the problem deeper.

### Example Output
```
./bench.py --bucket tempbucket --s5cmd master 478 --warmup 2 --runs 15
```

### Benchmark summary: 
|Scenarios | File Size | File Count |
|:---|:---|:---|
| small files | 1M | 10000 |
| large file | 10G | 1 |
| very large file | 300G | 1 |

|Scenario| Summary |
|:---|:---|
| upload small files | 'PR:478' ran 1.01 ± 0.02 times faster than 'master' |
| download small files | 'PR:478' ran 1.00 ± 0.01 times faster than 'master' |
| remove small files | 'master' ran 1.05 ± 0.41 times faster than 'PR:478' |
| upload large file | 'PR:478' ran 1.18 ± 0.23 times faster than 'master' |
| download large file | 'master' ran 1.05 ± 0.08 times faster than 'PR:478' |
| remove large file | 'master' ran 1.21 ± 0.39 times faster than 'PR:478' |
| upload very large file | 'PR:478' ran 1.01 times faster than 'master' |
| download very large file | 'master' ran 1.02 times faster than 'PR:478' |
| remove very large file | 'PR:478' ran 1.13 times faster than 'master' |

 ### Detailed summary: 
 |Scenario| Command | Mean [s] | Min [s] | Max [s] | Relative |
 |:---|:---|---:|---:|---:|---:|
| upload small files | `PR:478` | 9.117 ± 0.155 | 8.848 | 9.337 | 1.00 |
| upload small files | `master` | 9.252 ± 0.160 | 9.084 | 9.483 | 1.01 ± 0.02 |
| download small files | `PR:478` | 79.992 ± 0.091 | 79.879 | 80.177 | 1.00 |
| download small files | `master` | 79.993 ± 0.462 | 79.096 | 81.028 | 1.00 ± 0.01 |
 | remove small files | `PR:478` | 2.603 ± 0.435 | 2.308 | 3.245 | 1.05 ± 0.41 |
 | remove small files | `master` | 2.470 ± 0.878 | 2.012 | 3.787 | 1.00 |
 | upload large file | `PR:478` | 10.093 ± 1.491 | 9.043 | 14.222 | 1.00 |
 | upload large file | `master` | 11.876 ± 1.486 | 10.730 | 15.787 | 1.18 ± 0.23 |
 | download large file | `PR:478` | 27.689 ± 1.378 | 25.979 | 30.803 | 1.05 ± 0.08 |
 | download large file | `master` | 26.452 ± 1.667 | 24.891 | 29.375 | 1.00 |
 | remove large file | `PR:478` | 0.157 ± 0.029 | 0.122 | 0.210 | 1.21 ± 0.39 |
 | remove large file | `master` | 0.130 ± 0.034 | 0.090 | 0.220 | 1.00 |
 | upload very large file | `PR:478` | 270.462 | 270.462 | 270.462 | 1.00 |
 | upload very large file | `master` | 272.473 | 272.473 | 272.473 | 1.01 |
 | download very large file | `PR:478` | 2538.727 | 2538.727 | 2538.727 | 1.02 |
 | download very large file | `master` | 2501.010 | 2501.010 | 2501.010 | 1.00 |
 | remove very large file | `PR:478` | 1.011 | 1.011 | 1.011 | 1.00 |
 | remove very large file | `master` | 1.145 | 1.145 | 1.145 | 1.13 |
