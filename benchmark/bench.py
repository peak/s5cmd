#!/usr/bin/env python
import argparse
import re
import shutil
import os
import subprocess
from tempfile import mkdtemp


def main(argv=None):
    parser = argparse.ArgumentParser(description='Compare performance of two different builds of s5cmd.')

    parser.add_argument('-s', '--s5cmd', nargs=2, metavar=("OLD", "NEW"), default=('v1.4.0', 'v2.0.0'),
                        help='Reference to old and new s5cmd.'
                             ' It can be a decimal indicating PR number, '
                             'any of the version tags like v2.0.0 or commit tag.')
    parser.add_argument('-w', '--warmup', default=2, help='Number of program executions before the actual benchmark:')
    parser.add_argument('-r', '--runs', default=10, help='Number of runs to perform for each command')
    parser.add_argument('-b', '--bucket', required=True, help='Name of the bucket in remote')
    parser.add_argument('-p', '--prefix', default='s5cmd-benchmarks-',
                        help='Key prefix to be used while uploading to a specified bucket')
    parser.add_argument('-hf', '--hyperfine-extra-flags',
                        help='hyperfine global extra flags. '
                             'Write in between quotation marks '
                             'and start with a space to avoid bugs.')
    parser.add_argument('-sf', '--s5cmd-extra-flags', default="",
                        help='s5cmd global extra flags. '
                             'Write in between quotation marks '
                             'and start with a space to avoid bugs.')

    args = parser.parse_args(argv)
    cwd = os.getcwd()

    local_dir, dst_path = create_bench_dir(args)
    old_s5cmd, new_s5cmd = build_s5cmd_exec(args.s5cmd[0], args.s5cmd[1], local_dir)

    init_bench_results(cwd)

    scenarios = [
        Scenario(
            name='upload small files',
            cwd=cwd,
            file_size='1M',
            file_count='10000',
            s5cmd_args=[args.s5cmd_extra_flags, 'cp', '\"*\"', f's3://{args.bucket}/{args.prefix}/1/{{dir}}/'],
            hyperfine_args=dict({'runs': args.runs, 'warmup': args.warmup, 'extra_flags': args.hyperfine_extra_flags}),
            local_dir=local_dir,
        ),
        # This scenario depends on the remote files uploaded by the scenario above.
        Scenario(
            name='download small files',
            cwd=cwd,
            file_size=None,
            file_count=None,
            s5cmd_args=[args.s5cmd_extra_flags, 'cp', f'\"s3://{args.bucket}/{args.prefix}/1/{{dir}}/*\"', f'1/'],
            hyperfine_args=dict({'runs': args.runs, 'warmup': args.warmup, 'extra_flags': args.hyperfine_extra_flags}),
            local_dir=local_dir,
        ),
        Scenario(
            name='upload large files',
            cwd=cwd,
            file_size='10G',
            file_count='1',
            s5cmd_args=[args.s5cmd_extra_flags, 'cp', '\"*\"',
                        f's3://{args.bucket}/{args.prefix}/2/{{dir}}/'],
            hyperfine_args=dict({'runs': args.runs, 'warmup': args.warmup, 'extra_flags': args.hyperfine_extra_flags}),
            local_dir=local_dir,
        ),

        # This scenario depends on the remote files uploaded by the scenario above.
        Scenario(
            name='download large files',
            cwd=cwd,
            file_size=None,
            file_count=None,
            s5cmd_args=[args.s5cmd_extra_flags, 'cp',
                        f'\"s3://{args.bucket}/{args.prefix}/2/{{dir}}/*\"', f'2/'],
            hyperfine_args=dict({'runs': args.runs, 'warmup': args.warmup, 'extra_flags': args.hyperfine_extra_flags}),
            local_dir=local_dir,
        ),
        # TODO: add prepare flag for scenarios that tests remove
        # This scenario depends on the remote files uploaded by the scenario 'upload small files'
        Scenario(
            name='remove small files',
            cwd=cwd,
            file_size=None,
            file_count=None,
            s5cmd_args=[args.s5cmd_extra_flags, 'rm', f's3://{args.bucket}/{args.prefix}/1/{{dir}}/*'],
            hyperfine_args=dict({'runs': 1, 'warmup': 0, 'extra_flags': args.hyperfine_extra_flags}),
            local_dir=local_dir,
        ),
        # This scenario depends on the remote files uploaded by the scenario 'upload large files'
        Scenario(
            name='remove large files',
            cwd=cwd,
            file_size=None,
            file_count=None,
            s5cmd_args=[args.s5cmd_extra_flags, 'rm', f's3://{args.bucket}/{args.prefix}/2/{{dir}}/*'],
            hyperfine_args=dict({'runs': 1, 'warmup': 0, 'extra_flags': args.hyperfine_extra_flags}),
            local_dir=local_dir,
        ),
    ]
    for scenario in scenarios:
        # Any scenario that needs to download from remote
        # has to be executed after an upload test, as upload creates
        # local files, and download can use
        scenario.setup()
        scenario.run(old_s5cmd, new_s5cmd)
        scenario.teardown()

    # append detailed_summary to summary.md
    with open(f'{cwd}/detailed_summary.md', 'r+') as f:
        detailed_summary = join_with_spaces(f.readlines())
    with open(f'{cwd}/summary.md', 'a') as f:
        f.write(detailed_summary)

    cleanup(local_dir, cwd)

    return 0


class S5cmd:
    def __init__(self, folder_path, clone_path, name, tag):
        self.clone_path = clone_path
        self.name = name
        self.tag = tag
        self.git_type = ""
        self.path = f'{folder_path}/{self.name}'
        self.build()

    def build(self):

        if re.match('^[0-9]+$', self.tag):
            run_cmd(['git', '-C', f'{self.clone_path}', 'fetch', 'origin', f'pull/{self.tag}/head', '-q'])
            self.git_type = 'PR'
        elif re.match('^v([0-9]+\.){2}([0-9])(-[a-z]*\.?[0-9]?)?$', self.tag):
            run_cmd(['git', '-C', f'{self.clone_path}', 'checkout', f'tags/{self.tag}', '-q'])
            self.git_type = 'version'
        else:
            run_cmd(['git', '-C', f'{self.clone_path}', 'checkout', f'{self.tag}', '-q'])
            self.git_type = 'commit'

        os.chdir(self.clone_path)
        run_cmd(['go', 'build', '-o', f'{self.path}'])


class Scenario:
    def __init__(self, name, cwd, file_size, file_count, s5cmd_args, hyperfine_args, local_dir):
        self.name = name
        self.cwd = cwd
        self.file_size = file_size
        self.file_count = file_count
        self.s5cmd_args = s5cmd_args
        self.hyperfine_args = hyperfine_args
        self.local_dir = local_dir
        self.folder_dir = ""

    def setup(self):

        if self.file_count:
            self.file_count = int(self.file_count)
            self.create_files()
        else:
            self.folder_dir = f'{self.local_dir}/'

    def create_files(self):
        # create subdirectory under local_dir named with a scenario name
        # create file_count files with each file_size size
        self.folder_dir = f'{self.local_dir}/{self.name.replace(" ", "-")}'

        os.mkdir(self.folder_dir)
        os.chdir(self.folder_dir)

        if self.file_count <= 0:
            raise ValueError(f"{self.file_count} cannot be negative.")
        elif self.file_count == 1:
            run_cmd(['dd',
                     'if=/dev/urandom',
                     'of=tmp',
                     'status=none',
                     'bs=1M',
                     f'count={int(to_bytes(self.file_size) / (1024 ** 2))}'])

        else:
            # create one big file first, then split it into
            # smaller pieces. This reduces time consumption.
            temp_bigfile_dir = self.folder_dir + '/tmp'
            large_file_size = to_bytes(self.file_size) * self.file_count
            run_cmd(['dd',
                     'if=/dev/urandom',
                     'of=tmp',
                     'status=none',
                     'bs=1M',
                     f'count={int(large_file_size / (1024 ** 2))}'])
            run_cmd(['split',
                     '-a',
                     '4',
                     '-n',
                     f'{self.file_count}',
                     f'{temp_bigfile_dir}',
                     'tmp'
                     ])
            os.remove(temp_bigfile_dir)

    def teardown(self):
        # if local files are created, remove at teardown
        if self.file_count:
            shutil.rmtree(self.folder_dir)

    def run(self, old_s5cmd, new_s5cmd):
        old_name = f'{old_s5cmd.git_type}:{old_s5cmd.tag}'
        new_name = f'{new_s5cmd.git_type}:{new_s5cmd.tag}'
        print(f'{self.name}: ')

        os.chdir(self.folder_dir)

        cmd = [
            'hyperfine',
            f'--export-markdown',
            f'{self.local_dir}/temp.md',
            '--runs',
            f'{self.hyperfine_args["runs"]}',
            '--warmup',
            f'{self.hyperfine_args["warmup"]}',
            '--parameter-list',
            'dir',
            'old,new',
            '-n', f'{old_name}',
            '-n', f'{new_name}',
            f"{old_s5cmd.path} {join_with_spaces(self.s5cmd_args)}",
        ]
        if self.hyperfine_args["extra_flags"]:
            cmd.append(self.hyperfine_args["extra_flags"].strip())

        output = run_cmd(cmd)
        summary = self.parse_output(output)
        with open(f"{self.cwd}/summary.md", "a") as f:
            f.write(summary)

        detailed_summary = ""
        with open(f"{self.local_dir}/temp.md", "r+") as f:
            lines = f.readlines()
            # get markdown table and add a new column in the front as scenario name
            detailed_summary = f"| {self.name} {join_with_spaces(lines[-1])}" \
                               f"| {self.name} {join_with_spaces(lines[-2])}"

        with open(f"{self.cwd}/detailed_summary.md", "a") as f:
            f.write(detailed_summary)

    def parse_output(self, output):
        lines = output.split('\n')
        summary = ""
        for i, line in enumerate(lines):
            # get the next two lines after summary and format it as markdown table.
            if 'Summary' in line:
                line1 = lines[i + 1].replace("\n", "").strip()
                line2 = lines[i + 2].replace("\n", "").strip()
                summary = f"| {self.name} | {line1} {line2} |\n"
        return summary


def init_bench_results(cwd):
    summary = "### Benchmark summary: " \
              "\n|Scenario| Summary |" \
              "\n|:---|:---|" \
              "\n"
    with open(f"{cwd}/summary.md", "w") as file:
        file.write(summary)

    detailed_summary = '\n### Detailed summary: ' \
                       '\n|Scenario| Command | Mean [ms] | Min [ms] | Max [ms] | Relative |' \
                       '\n|:---|:---|---:|---:|---:|---:|' \
                       '\n'

    with open(f"{cwd}/detailed_summary.md", "w") as file:
        file.write(detailed_summary)


def join_with_spaces(lst):
    return " ".join(lst)


def to_bytes(size):
    if size.isdigit():
        return int(size)
    unit = size[-1]
    if unit == 'K':
        return int(size[:-1]) * 1024
    elif unit == 'M':
        return int(size[:-1]) * (1024 ** 2)
    elif unit == 'G':
        return int(size[:-1]) * (1024 ** 3)
    elif unit == 'T':
        return int(size[:-1]) * (1024 ** 4)
    elif unit == 'P':
        return int(size[:-1]) * (1024 ** 5)
    else:
        raise ValueError('Given size is not correct.')


def build_s5cmd_exec(old, new, local_dir):
    run_cmd(['git', '-C', f'{local_dir}', 'clone', 'https://github.com/peak/s5cmd.git', '-q'])

    clone_dir = f'{local_dir}/s5cmd/'

    old = S5cmd(local_dir, clone_dir, 'old', old)
    new = S5cmd(local_dir, clone_dir, 'new', new)
    return old, new


def create_bench_dir(args):
    local_dir = mkdtemp(prefix=args.prefix)
    dst_path = f's3://{args.bucket}/{args.prefix}'
    print(f'All the local temporary files will be created at {local_dir}')
    print(f'All the remote files will be uploaded to {dst_path}')
    print(f'The created local&remote files will be deleted at the end of tests.')
    print(
        f'Hyperfine will execute s5cmd uploads {args.warmup} times to warmup, and {args.runs} times for measurements.')

    return local_dir, dst_path


def run_cmd(cmd):
    process = subprocess.run(cmd, capture_output=True, text=True)
    print(process.stderr, end='')
    print(process.stdout, end='')
    process.check_returncode()
    return process.stdout


def cleanup(tmp_dir, temp_result_file_dir):
    if os.path.isfile(f'{temp_result_file_dir}/detailed_summary.md'):
        os.remove(f'{temp_result_file_dir}/detailed_summary.md')
    shutil.rmtree(tmp_dir)


if __name__ == "__main__":
    raise SystemExit(main())
