#! /bin/bash

# constants
start_dir=$(pwd)
large_file_size=10G
smaller_to_large=10000

old_exec_name=olds5cmd
new_exec_name=news5cmd

# default values of options/flags
warmup_count=2
run_count=10
bucket=example
key_prefix=benchmark
new=v2.0.0
old=v1.4.0

# read options
while getopts b:k:w:r:o:n: flag
do
    case "${flag}" in
        b) bucket=${OPTARG};;
        k) key_prefix=${OPTARG};;
        w) warmup_count=${OPTARG};;
        r) run_count=${OPTARG};;
        o) old=${OPTARG};;
        n) new=${OPTARG};;
        *) echo "Invalid flag(s) are used"
    esac
done
dst_prefix=s3://${bucket}/${key_prefix}


# create the temporary directory
tmp_dir=$(mktemp -d -t s5cmd-benchmark-XXXXXXXXXX)
echo "All the local temporary files will be created at $tmp_dir."
echo "All the remote files will be uploaded to the "'"'"$bucket"'"'" bucket with key prefix of "'"'"$key_prefix"'"'"."
echo "The created local&remote files will be deleted at the end of tests."
echo "Hyperfine will execute s5cmd uploads $warmup_count times to warmup, and $run_count times for measurements."


## Git clone & build
echo "Started cloning and building the project from $old and $new."
cd "$tmp_dir" || exit 
git clone https://github.com/peak/s5cmd.git -q
cd s5cmd || exit

## creates executables from the tags/commits/PR to parent directory
function prepare_exec {
if [[ "$1" =~ ^[0-9]+$ ]] 
then
   git pull origin pull/$1/head -q
   echo "peak:#$1 decimal $2"
elif [[ "$1" =~ ^v([0-9]+\.){2}([0-9])(-[a-z]*\.?\d?)?$ ]]
then
   git checkout "tags/$1" -q
   echo "$1 version $2"
else
   git checkout "$1" -q
   echo "$1 commit-tag $2"
fi
go build -o "../$2"
} 

## create executables
prepare_exec "$old" $old_exec_name
prepare_exec "$new" $new_exec_name

echo "Completed cloning and building the project from $old and $new."
# shellcheck disable=SC2164
cd "$start_dir"

# create temporary files
## one file of large size 
large_file_dir=${tmp_dir}/${large_file_size}
mkdir "$large_file_dir"
large_file=${large_file_dir}/${large_file_size}
### create the large file
case "$OSTYPE" in
  darwin*)  mkfile -n $large_file_size ${large_file};; 
  linux*)   truncate --size $large_file_size ${large_file};;
  msys*)    echo "This script cannot run in Windows" && exit ;;
  cygwin*)  echo "This script cannot run in Windows" && exit ;;
  *)   truncate --size $large_file_size ${large_file};;
  # one should not use Windows. I don't want to manually parse $large_file_size!
  # fsutil file createnew (?) only accepts numbers, without "humanized" suffices! 
esac

## create smaller files from the large file
small_file_dir=${tmp_dir}/small
mkdir $small_file_dir
small_file=${small_file_dir}/small
split -n $smaller_to_large $large_file  $small_file

## make the tests!
### large file upload
echo; echo "Upload the large file of size $large_file_size:"
large_first_dst=${dst_prefix}/large1/
large_second_dst=${dst_prefix}/large2/
large_first_up="$tmp_dir/$old_exec_name  cp ${large_file} $large_first_dst"
large_second_up="$tmp_dir/$new_exec_name cp ${large_file} $large_second_dst"

hyperfine  --warmup $warmup_count --runs $run_count "$large_first_up" "$large_second_up"

### small file upload
echo; echo "Upload $smaller_to_large small files:"
small_first_dst=${dst_prefix}/small1/
small_second_dst=${dst_prefix}/small2/
small_files="${small_file}*"
small_first_up="$tmp_dir/$old_exec_name   cp "'"'${small_files}'"'" $small_first_dst"
small_second_up="$tmp_dir/$new_exec_name  cp "'"'${small_files}'"'" $small_second_dst"

hyperfine --warmup $warmup_count --runs $run_count "$small_first_up" "$small_second_up"

# We can download to the same directory that we uploaded the files from, and we will.
# Both of them writes to the same directory, but, for now, I don't care. 
### large file download
echo; echo "Download the large file of size $large_file_size:"
large_first_dl="$tmp_dir/$old_exec_name  cp $large_first_dst* ${large_file_dir}/"
large_second_dl="$tmp_dir/$new_exec_name cp $large_second_dst* ${large_file_dir}/"

hyperfine --warmup $warmup_count --runs $run_count "$large_first_dl" "$large_second_dl"

### small file download
echo; echo "Download $smaller_to_large small files:"
small_first_dl="$tmp_dir/$old_exec_name   cp "'"'"$small_first_dst*"'"'" ${small_file}/"
small_second_dl="$tmp_dir/$new_exec_name  cp "'"'"$small_second_dst*"'"'" ${small_file}/"

hyperfine --warmup $warmup_count --runs $run_count  "$small_first_dl" "$small_second_dl"

### clear the remote files --iff bucket is unversioned, otherwise just puts
### delete marker(s), sorry about that!
echo; echo "Delete the large file of size $large_file_size from remote:"
large_first_rm="$tmp_dir/$old_exec_name rm ${large_first_dst}*"
large_second_rm="$tmp_dir/$new_exec_name rm ${large_second_dst}*"

# one can delete files once! So --warmup 0 --runs 1!
hyperfine --show-output --warmup 0 --runs 1 "$large_first_rm" "$large_second_rm"

echo; echo "Delete $smaller_to_large small files from remote:"
small_first_rm="$tmp_dir/$old_exec_name rm ${small_first_dst}*"
small_second_rm="$tmp_dir/$new_exec_name rm ${small_second_dst}*"

hyperfine --warmup 0 --runs 1 "$small_first_rm" "$small_second_rm"
 
# clear the temporary directories and files in local
echo; echo "Delete the temporary directories and files in local from $tmp_dir"
rm -rf  "$tmp_dir"