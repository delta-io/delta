#!/usr/bin/env python3

#
# Copyright (2021) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import os
import glob
import subprocess
import shlex
import shutil
from os import path

iceberg_lib_dir_name = "lib"
iceberg_src_dir_name = "iceberg_src"
iceberg_patches_dir_name = "iceberg_src_patches"

iceberg_src_commit_hash = "ede085d0f7529f24acd0c81dd0a43f7bb969b763"
iceberg_src_branch_with_commit_hash = "master"   # only this branch will be downloaded
iceberg_src_compiled_jar_rel_paths = [ # related to `iceberg_src_dir_name`
    "bundled-guava/build/libs/iceberg-bundled-guava-1.2.0-SNAPSHOT.jar",
    "common/build/libs/iceberg-common-1.2.0-SNAPSHOT.jar",
    "api/build/libs/iceberg-api-1.2.0-SNAPSHOT.jar",
    "core/build/libs/iceberg-core-1.2.0-SNAPSHOT.jar",
    "parquet/build/libs/iceberg-parquet-1.2.0-SNAPSHOT.jar",
]

iceberg_root_dir = path.abspath(path.dirname(__file__))
iceberg_src_dir = path.join(iceberg_root_dir, iceberg_src_dir_name)
iceberg_patches_dir = path.join(iceberg_root_dir, iceberg_patches_dir_name)
iceberg_lib_dir = path.join(iceberg_root_dir, iceberg_lib_dir_name)


def compile_jar_rel_path_to_lib_jar_path(jar_rel_path):
    jar_file_name = path.basename(path.normpath(jar_rel_path))
    jar_file_name_splits = path.splitext(jar_file_name)
    new_jar_file_name = "%s_%s%s" % (jar_file_name_splits[0], iceberg_src_commit_hash, jar_file_name_splits[1])
    return path.join(iceberg_lib_dir, new_jar_file_name)


def iceberg_jars_exists():
    for jar_rel_path in iceberg_src_compiled_jar_rel_paths:
        if not path.exists(compile_jar_rel_path_to_lib_jar_path(jar_rel_path)):
            return False
    return True


def prepare_iceberg_source():
    with WorkingDirectory(iceberg_root_dir):
        print(">>> Cloning Iceberg repo")
        shutil.rmtree(iceberg_src_dir_name, ignore_errors=True)
        run_cmd("git config user.email \"<>\"")
        run_cmd("git config user.name \"Anonymous\"")
        run_cmd("git clone --branch %s https://github.com/apache/iceberg.git %s" %
                (iceberg_src_branch_with_commit_hash, iceberg_src_dir_name))

    with WorkingDirectory(iceberg_src_dir):
        run_cmd("git config user.email \"<>\"")
        run_cmd("git config user.name \"Anonymous\"")
        run_cmd("git checkout %s" % iceberg_src_commit_hash)

        print(">>> Applying patch files")
        patch_files = glob.glob(path.join(iceberg_patches_dir, "*.patch"))
        patch_files.sort()

        for patch_file in patch_files:
            print(">>> Applying '%s'" % patch_file)
            run_cmd("git apply %s" % patch_file)
            run_cmd("git add .")
            run_cmd("git commit -a -m 'applied %s'" % path.basename(patch_file))


def generate_iceberg_jars():
    print(">>> Compiling JARs")
    with WorkingDirectory(iceberg_src_dir):
        # disable style checks (can fail with patches) and tests
        build_args = "-x spotlessCheck -x checkstyleMain -x test -x integrationTest"
        run_cmd("./gradlew :iceberg-core:build %s" % build_args)
        run_cmd("./gradlew :iceberg-parquet:build %s" % build_args)

    print(">>> Copying JARs to lib directory")
    shutil.rmtree(iceberg_lib_dir, ignore_errors=True)
    os.mkdir(iceberg_lib_dir)

    for compiled_jar_rel_path in iceberg_src_compiled_jar_rel_paths:
        compiled_jar_full_path = path.join(iceberg_src_dir, compiled_jar_rel_path)
        if not path.exists(compiled_jar_full_path):
            raise Exception("Could not find the jar " + compiled_jar_full_path)
        lib_jar_full_path = compile_jar_rel_path_to_lib_jar_path(compiled_jar_rel_path)
        shutil.copyfile(compiled_jar_full_path, lib_jar_full_path)

    if not iceberg_jars_exists():
        raise Exception("JAR copying failed")


def run_cmd(cmd, throw_on_error=True, env=None, stream_output=False, **kwargs):
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    cmd_env = os.environ.copy()
    if env:
        cmd_env.update(env)

    if stream_output:
        child = subprocess.Popen(cmd, env=cmd_env, **kwargs)
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception("Non-zero exitcode: %s" % (exit_code))
        print("----\n")
        return exit_code
    else:
        child = subprocess.Popen(
            cmd,
            env=cmd_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **kwargs)
        (stdout, stderr) = child.communicate()
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception(
                "Non-zero exitcode: %s\n\nSTDOUT:\n%s\n\nSTDERR:%s" %
                (exit_code, stdout, stderr))
        return (exit_code, stdout, stderr)


# pylint: disable=too-few-public-methods
class WorkingDirectory(object):
    def __init__(self, working_directory):
        self.working_directory = working_directory
        self.old_workdir = os.getcwd()

    def __enter__(self):
        os.chdir(self.working_directory)

    def __exit__(self, tpe, value, traceback):
        os.chdir(self.old_workdir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force",
        required=False,
        default=False,
        action="store_true",
        help="Force the generation even if already generated, useful for testing.")
    args = parser.parse_args()

    if args.force or not iceberg_jars_exists():
        prepare_iceberg_source()
        generate_iceberg_jars()
