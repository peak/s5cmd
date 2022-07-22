#!/bin/bash
#
# Perform benchmark tests by comparing two different builds of s5cmd.

# constants
readonly START_DIR=$(pwd)
readonly GO_REQ_VERSION=1.16.0
readonly HYPERFINE_REQ_VERSION=1.14.0
readonly OLD_EXEC_NAME=olds5cmd
readonly NEW_EXEC_NAME=news5cmd
readonly YELLOW='\033[1;33m'
readonly GREEN='\033[0;32m>>'
readonly RED='\033[0;31m'
readonly NOCOLOR='\033[0m'
readonly BENCH_RESULT=bench_results.md
readonly SUMMARY=summary.md

# default values of options/flags
WARMUP_COUNT=2
RUN_COUNT=10
BUCKET=example
KEY_PREFIX=benchmark
NEW=v2.0.0
OLD=v1.4.0
GLOBAL_FLAGS=""
HYPERFINE_FLAGS=""
LARGE_FILE_SIZE=1G
SMALL_FILE_COUNT=100

main() {
	read_options "$@"

	check_version "go version" $GO_REQ_VERSION
	check_version "hyperfine --version" $HYPERFINE_REQ_VERSION

	create_benchmark_dir

	build_s5cmd_exec

	create_temp_files

	make_test

	cleanup

	echo -e "${GREEN}Benchmark completed. Benchmark results are saved in $START_DIR/$BENCH_RESULT ${NOCOLOR}"
}

read_options() {
	while getopts b:k:w:r:o:n:h:g:s:c: flag; do
		case "${flag}" in
		b) BUCKET=${OPTARG} ;;
		k) KEY_PREFIX=${OPTARG} ;;
		w) WARMUP_COUNT=${OPTARG} ;;
		r) RUN_COUNT=${OPTARG} ;;
		o) OLD=${OPTARG} ;;
		n) NEW=${OPTARG} ;;
		h) HYPERFINE_FLAGS=${OPTARG} ;;
		g) GLOBAL_FLAGS=${OPTARG} ;;
		s) LARGE_FILE_SIZE=${OPTARG} ;;
		c) SMALL_FILE_COUNT=${OPTARG} ;;

		*) echo "Invalid flag(s) are used" ;;
		esac
	done
	readonly BUCKET
	readonly KEY_PREFIX
	readonly WARMUP_COUNT
	readonly RUN_COUNT
	readonly OLD
	readonly NEW
	readonly LARGE_FILE_SIZE
	readonly SMALL_FILE_COUNT
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
	echo -e "${YELLOW}All the local temporary files will be created at $tmp_dir."
	echo -e "All the remote files will be uploaded to the "'"'"$BUCKET"'"'" bucket with key prefix of "'"'"$KEY_PREFIX"'"'"."
	echo -e "The created local&remote files will be deleted at the end of tests."
	echo -e "Hyperfine will execute s5cmd uploads $WARMUP_COUNT times to warmup, and $RUN_COUNT times for measurements.${NOCOLOR}"
}

check_type() {
	if [[ "$1" =~ ^[0-9]+$ ]]; then
		type=PR
	elif [[ "$1" =~ ^v([0-9]+\.){2}([0-9])(-[a-z]*\.?[0-9]?)?$ ]]; then
		type=version
	else
		git checkout "$1" -q
		type=commit
	fi
	echo $type
}

## creates executables from the tags/commits/PR to parent directory
prepare_exec() {
	if [[ "$1" = PR ]]; then
		git fetch origin pull/$2/head -q
	elif [[ "$1" == version ]]; then
		git checkout "tags/$2" -q
	else
		git checkout "$2" -q
	fi
	go build -o "../$3"
}

## git clone & build s5cmd, and user defined old and new executables.
build_s5cmd_exec() {
	echo
	type_old=$(check_type $OLD)
	type_new=$(check_type $NEW)

	echo "Started cloning and building the project from $type_old:$OLD and $type_new:$NEW."
	cd "$tmp_dir" || exit
	git clone https://github.com/peak/s5cmd.git -q
	cd s5cmd || exit
	## create executables
	prepare_exec $type_old $OLD $OLD_EXEC_NAME
	prepare_exec $type_new $NEW $NEW_EXEC_NAME

	echo -e "${GREEN}Completed cloning and building the project from $type_old:$OLD and $type_new:$NEW.${NOCOLOR}"
}

create_temp_files() {
	# shellcheck disable=SC2164
	cd "$START_DIR"
	echo "Creating temporary files..."
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
	split -a 3 -n $SMALL_FILE_COUNT $large_file $small_file
	echo -e "${GREEN}Created temporary files in $tmp_dir${NOCOLOR}"
}

print_info() {
	echo
	if [[ "$2" == large ]]; then
		echo -e "${YELLOW}$1 the large file of size $LARGE_FILE_SIZE:${NOCOLOR}"
	elif [[ "$2" == small ]]; then
		echo -e "${YELLOW}$1 $SMALL_FILE_COUNT small files:${NOCOLOR}"
	fi
}

save_summary() {
	sed -i -e "1,2d" tmp.md
	sed -i -e "s/^/|$1 $2/" tmp.md
	cat tmp.md >> $SUMMARY
}

save_result() {
	result=$(grep -B 1 "faster than" tmp.txt | tr -d '\n')
	echo "|$1 $2|$result|" >> $BENCH_RESULT
}


upload() {
	print_info Upload $1
	first_dst=${dst_prefix}/${1}1/
	second_dst=${dst_prefix}/${1}2/
	first_up="$tmp_dir/$OLD_EXEC_NAME  $GLOBAL_FLAGS cp "'"'${2}'"'" $first_dst"
	second_up="$tmp_dir/$NEW_EXEC_NAME $GLOBAL_FLAGS cp "'"'${2}'"'" $second_dst"

	hyperfine --export-markdown tmp.md $HYPERFINE_FLAGS \
	--warmup $WARMUP_COUNT --runs $RUN_COUNT -n $type_old:$OLD \
	"$first_up" -n $type_new:$NEW "$second_up" | tee tmp.txt
	
	save_result Upload $1
	save_summary Upload $1 
}

download() {
	# We can download to the same directory that we uploaded the files from, and we will.
	# Both of them writes to the same directory, but, for now, I don't care.
	print_info Download $1 
	first_dst=${dst_prefix}/${1}1/*
	second_dst=${dst_prefix}/${1}2/*
	first_dl="$tmp_dir/$OLD_EXEC_NAME  $GLOBAL_FLAGS cp "'"'$first_dst'"'" $2/"
	second_dl="$tmp_dir/$NEW_EXEC_NAME $GLOBAL_FLAGS cp "'"'$second_dst'"'" $2/"

	hyperfine --export-markdown tmp.md $HYPERFINE_FLAGS \
	--warmup $WARMUP_COUNT --runs $RUN_COUNT -n $type_old:$OLD \
	"$first_dl" -n $type_new:$NEW "$second_dl" | tee tmp.txt

	save_result Download $1
	save_summary Download $1 
}

remove() {
	### clear the remote files --iff bucket is unversioned, otherwise just puts
	### delete marker(s), sorry about that!
	print_info Remove $1 
	first_dst=${dst_prefix}/${1}1/*
	second_dst=${dst_prefix}/${1}2/*
	first_rm="$tmp_dir/$OLD_EXEC_NAME $GLOBAL_FLAGS rm ${first_dst}"
	second_rm="$tmp_dir/$NEW_EXEC_NAME $GLOBAL_FLAGS rm ${second_dst}"

	# one can delete files once! So --warmup 0 --runs 1!
	hyperfine --export-markdown tmp.md $HYPERFINE_FLAGS \
	--warmup 0 --runs 1 -n $type_old:$OLD "$first_rm"\
	 -n $type_new:$NEW "$second_rm" | tee tmp.txt

	save_result Remove $1
	save_summary Remove $1
}
init_bench_results() {

	# initialize bench results
	touch $BENCH_RESULT
	echo "$header"$'\n'"$header2" >$BENCH_RESULT
	echo "### Benchmark summary:"$'\n'"Large File Size: "\
	$LARGE_FILE_SIZE$'\n'"Number of Small Files: \
	"$SMALL_FILE_COUNT$'\n'$'\n'"|Scenario| Summary |"\
	$'\n'"|:---|:---|" > $BENCH_RESULT

	# initialize detailed summary
	header="### Detailed summary:"
	header2="|Scenario| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |"
	header3="|:---|:---|---:|---:|---:|---:|"
	touch $SUMMARY
	echo $'\n'$header$'\n'"$header2"$'\n'"$header3" >$SUMMARY
}

## Make the tests!
make_test() {

	init_bench_results

	upload Large $large_file
	upload Small ${small_file}"*"

	download Large $large_file_dir
	download Small $small_file_dir

	remove Large
	remove Small

	cat $SUMMARY >> $BENCH_RESULT
}

cleanup() {
	# clear the temporary directories and files in local
	echo
	echo "Deleting the temporary directories and files in local from $tmp_dir..."
	rm tmp.md*
	rm -rf "$tmp_dir"
	rm tmp.txt
	rm $SUMMARY

	echo -e "${GREEN}Deleted the temporary directories and files.${NOCOLOR}"
}

main "$@"
exit
