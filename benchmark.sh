#!/bin/bash
#
# Perform benchmark tests by comparing two different builds of s5cmd.

# constants
readonly START_DIR=$(pwd)
readonly GO_REQ_VERSION=1.16.0
readonly HYPERFINE_REQ_VERSION=1.14.0
readonly LARGE_FILE_SIZE=10G
readonly SMALLER_TO_LARGE=10000
readonly OLD_EXEC_NAME=olds5cmd
readonly NEW_EXEC_NAME=news5cmd

# default values of options/flags
WARMUP_COUNT=2
RUN_COUNT=10
BUCKET=example
KEY_PREFIX=benchmark
NEW=v2.0.0
OLD=v1.4.0
GLOBAL_FLAGS=""
HYPERFINE_FLAGS=""

main() {
   read_options "$@"

   check_version "go version" $GO_REQ_VERSION
   check_version "hyperfine --version" $HYPERFINE_REQ_VERSION

   create_benchmark_dir

   build_s5cmd_exec

   create_temp_files

   make_test

   cleanup
}

read_options() {
   while getopts b:k:w:r:o:n:f:h: flag; do
      case "${flag}" in
      b) BUCKET=${OPTARG} ;;
      k) KEY_PREFIX=${OPTARG} ;;
      w) WARMUP_COUNT=${OPTARG} ;;
      r) RUN_COUNT=${OPTARG} ;;
      o) OLD=${OPTARG} ;;
      n) NEW=${OPTARG} ;;
      h) HYPERFINE_FLAGS=${OPTARG} ;;
      f) GLOBAL_FLAGS=${OPTARG} ;;
      *) echo "Invalid flag(s) are used" ;;
      esac
   done
   readonly BUCKET
   readonly KEY_PREFIX
   readonly WARMUP_COUNT
   readonly RUN_COUNT
   readonly OLD
   readonly NEW
}

## check version of a program and exit if it is less than required version
check_version() {
   currentver=$(grep -Eo '([0-9]+\.){2}([0-9])' <<<$($1))
   requiredver=$2
   if [ "$(printf '%s\n' "$requiredver" "$currentver" | sort -V | head -n1)" != "$requiredver" ]; then
      echo "Less than ${requiredver}"
      exit
   fi
}

## create the temporary benchmark directory
create_benchmark_dir() {
   tmp_dir=$(mktemp -d -t s5cmd-benchmark-XXXXXXXXXX)
   dst_prefix=s3://${BUCKET}/${KEY_PREFIX}
   echo "All the local temporary files will be created at $tmp_dir."
   echo "All the remote files will be uploaded to the "'"'"$BUCKET"'"'" bucket with key prefix of "'"'"$KEY_PREFIX"'"'"."
   echo "The created local&remote files will be deleted at the end of tests."
   echo "Hyperfine will execute s5cmd uploads $WARMUP_COUNT times to warmup, and $RUN_COUNT times for measurements."
}

## creates executables from the tags/commits/PR to parent directory
prepare_exec() {
   if [[ "$1" =~ ^[0-9]+$ ]]; then
      git pull origin pull/$1/head -q
      echo "peak:#$1 decimal $2"
   elif [[ "$1" =~ ^v([0-9]+\.){2}([0-9])(-[a-z]*\.?\d?)?$ ]]; then
      git checkout "tags/$1" -q
      echo "$1 version $2"
   else
      git checkout "$1" -q
      echo "$1 commit-tag $2"
   fi
   go build -o "../$2"
}

## git clone & build s5cmd, and user defined old and new executables.
build_s5cmd_exec() {
   echo "Started cloning and building the project from $OLD and $NEW."
   cd "$tmp_dir" || exit
   git clone https://github.com/peak/s5cmd.git -q
   cd s5cmd || exit
   ## create executables
   prepare_exec "$OLD" $OLD_EXEC_NAME
   prepare_exec "$NEW" $NEW_EXEC_NAME
}

create_temp_files() {
   # shellcheck disable=SC2164
   cd "$START_DIR"

   # create temporary files
   ## one file of large size
   large_file_dir=${tmp_dir}/${LARGE_FILE_SIZE}
   mkdir "$large_file_dir"
   large_file=${large_file_dir}/${LARGE_FILE_SIZE}
   ### create the large file
   case "$OSTYPE" in
   darwin*) mkfile -n $LARGE_FILE_SIZE ${large_file} ;;
   linux*) truncate --size $LARGE_FILE_SIZE ${large_file} ;;
   msys*) echo "This script cannot run in Windows" && exit ;;
   cygwin*) echo "This script cannot run in Windows" && exit ;;
   *) truncate --size $LARGE_FILE_SIZE ${large_file} ;;
      # one should not use Windows. I don't want to manually parse $LARGE_FILE_SIZE!
      # fsutil file createnew (?) only accepts numbers, without "humanized" suffices!
   esac

   ## create smaller files from the large file
   small_file_dir=${tmp_dir}/small
   mkdir $small_file_dir
   small_file=${small_file_dir}/small
   split -a 3 -n $SMALLER_TO_LARGE $large_file $small_file
}

print_info() {
   echo
   if [[ "$1" == large ]]; then
      echo "$2 the large file of size $LARGE_FILE_SIZE:"
   elif [[ "$1" == small ]]; then
      echo "$2 $SMALLER_TO_LARGE small files:"
   fi
}

upload() {
   print_info $1 Upload
   first_dst=${dst_prefix}/${1}1/
   second_dst=${dst_prefix}/${1}2/
   first_up="$tmp_dir/$OLD_EXEC_NAME  $GLOBAL_FLAGS cp "'"'${2}'"'" $first_dst"
   second_up="$tmp_dir/$NEW_EXEC_NAME $GLOBAL_FLAGS cp "'"'${2}'"'" $second_dst"

   hyperfine "$HYPERFINE_FLAGS" --warmup $WARMUP_COUNT --runs $RUN_COUNT "$first_up" "$second_up"
}

download() {
   # We can download to the same directory that we uploaded the files from, and we will.
   # Both of them writes to the same directory, but, for now, I don't care.
   print_info $1 Download
   first_dst=${dst_prefix}/${1}1/*
   second_dst=${dst_prefix}/${1}2/*
   first_dl="$tmp_dir/$OLD_EXEC_NAME  $GLOBAL_FLAGS cp "'"'$first_dst'"'" $2/"
   second_dl="$tmp_dir/$NEW_EXEC_NAME $GLOBAL_FLAGS cp "'"'$second_dst'"'" $2/"

   hyperfine "$HYPERFINE_FLAGS" --warmup $WARMUP_COUNT --runs $RUN_COUNT "$first_dl" "$second_dl"
}

remove() {
   ### clear the remote files --iff bucket is unversioned, otherwise just puts
   ### delete marker(s), sorry about that!
   print_info $1 Remove
   first_dst=${dst_prefix}/${1}1/*
   second_dst=${dst_prefix}/${1}2/*
   first_rm="$tmp_dir/$OLD_EXEC_NAME $GLOBAL_FLAGS rm ${first_dst}"
   second_rm="$tmp_dir/$NEW_EXEC_NAME $GLOBAL_FLAGS rm ${second_dst}"

   # one can delete files once! So --warmup 0 --runs 1!
   hyperfine "$HYPERFINE_FLAGS" --warmup 0 --runs 1 "$first_rm" "$second_rm"
}

## Make the tests!
make_test() {

   upload large $large_file
   upload small ${small_file}*

   download large $large_file_dir
   download small $small_file_dir

   remove large
   remove small

}

cleanup() {
   # clear the temporary directories and files in local
   echo
   echo "Delete the temporary directories and files in local from $tmp_dir"
   rm -rf "$tmp_dir"
}

main "$@"
exit