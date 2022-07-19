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
    esac
done
dst_prefix=s3://${bucket}/${key_prefix}


# create the temporary directory
tmp_dir=$(mktemp -d -t s5cmd-benchmark-XXXXXXXXXX)
echo "All the local temporary files will be created at $tmp_dir, they will be deleted after the execution."
echo "All the remote files will be uploaded to the "'"'"$bucket"'"'" bucket with key prefix of "'"'"$key_prefix"'"'"."
echo "Hyperfine will execute s5cmd uploads $warmup_count times to warmup, and $run_count times for measurements."


## Git clone & build
echo Started cloning and building the project from $old and $new.
cd $tmp_dir
git clone https://github.com/peak/s5cmd.git -q
cd s5cmd

## creates executables from the tags/commits/PR to parent directory
function prepare_exec {
if [[ "$1" =~ ^[0-9]+$ ]] 
then
   git pull origin pull/$1/head -q
   echo $1 decimal $2
elif [[ "$1" =~ ^v([0-9]+\.){2}([0-9])(-[a-z]*\.?\d?)?$ ]]
then
   git checkout tags/$1 -q
   echo $1 version $2
else
   git checkout $1 -q
   echo $1 commit-tag $2
fi
go build -o ../$2
} 

## create executables
prepare_exec "$old" $old_exec_name
prepare_exec "$new" $new_exec_name

echo Completed cloning and building the project from $old and $new.
cd $start_dir

# create temporary files
## one file of large size 
large_file_dir=${tmp_dir}/${large_file_size}
mkdir $large_file_dir
large_file=${large_file_dir}/${large_file_size}
truncate --size $large_file_size ${large_file}

## create smaller files from the large file
small_file_dir=${tmp_dir}/small
mkdir $small_file_dir
small_file=${small_file_dir}/small
split -n $smaller_to_large $large_file  $small_file

## make the tests!
### large file upload
echo "Upload the large file of size $large_file_size:"
large_first_dst=${dst_prefix}/large1/
large_second_dst=${dst_prefix}/large2/
large_first="$tmp_dir/$old_exec_name  cp ${large_file} $large_first_dst"
large_second="$tmp_dir/$new_exec_name cp ${large_file} $large_second_dst"

hyperfine --warmup $warmup_count --runs $run_count  -n "Old version of $old" "$large_first" -n "New version of $new" "$large_second"

### small file upload
echo "Upload $smaller_to_large small files:"
small_first_dst=${dst_prefix}/small1/
small_second_dst=${dst_prefix}/small2/
small_files="${small_file}*"
small_first="$tmp_dir/$old_exec_name   cp "'"'${small_files}'"'" $small_first_dst"
small_second="$tmp_dir/$new_exec_name  cp "'"'${small_files}'"'" $small_second_dst"

hyperfine --warmup $warmup_count --runs $run_count -n "Old version of $old" "$small_first" -n "New version of $new" "$small_second"

# clear the remote files --all versions!
s5cmd --stat --log error rm "${large_first_dst}*"
s5cmd --stat --log error rm "${large_second_dst}*"
s5cmd --stat --log error rm "${small_first_dst}*"
s5cmd --stat --log error rm "${small_second_dst}*"
 
# clear the temporary directories and files in local
rm -rf  $tmp_dir