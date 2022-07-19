#! /bin/bash

# constants
large_file_size=10G
smaller_to_large=10000
warmup_count=2
run_count=3
bucket=example
key_prefix=benchmark

while getopts b:k:w:r: flag
do
    case "${flag}" in
        b) bucket=${OPTARG};;
        k) key_prefix=${OPTARG};;
        w) warmup_count=${OPTARG};;
        r) run_count=${OPTARG};;
    esac
done

dst_prefix=s3://${bucket}/${key_prefix}


# create the temporary directory
tmp_dir=$(mktemp -d -t s5cmd-benchmark-XXXXXXXXXX)
echo "All the local temporary files will be created at $tmp_dir, they will be deleted after the execution."
echo "All the remote files will be uploaded to the "'"'"$bucket"'"'" bucket with key prefix of "'"'"$key_prefix"'"'"."
echo "Hyperfine will execute s5cmd uploads $warmup_count times to warmup, and $run_count times for measurements."


# create temporary files
## one file of size 
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
large_first="s5cmd   cp ${large_file} $large_first_dst"
large_second="./s5cmd cp ${large_file} $large_second_dst"

hyperfine --warmup $warmup_count --runs $run_count "$large_first" "$large_second"

### small file upload
echo "Upload $smaller_to_large small files:"
small_first_dst=${dst_prefix}/small1/
small_second_dst=${dst_prefix}/small2/
small_files="${small_file}*"
small_first="s5cmd   cp "'"'"${small_files}"'"'" $small_first_dst"
small_second="./s5cmd cp "'"'"${small_files}"'"'" $small_second_dst"

hyperfine --warmup $warmup_count --runs $run_count "$small_first" "$small_second"

# clear the remote files --all versions!
s5cmd --stat --log error rm "${large_first_dst}*"
s5cmd --stat --log error rm "${large_second_dst}*"
s5cmd --stat --log error rm "${small_first_dst}*"
s5cmd --stat --log error rm "${small_second_dst}*"
 
# clear the temporary directories and files in local
rm -rf  $tmp_dir