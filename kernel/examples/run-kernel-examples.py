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

'''
To run examples by building the artifacts from code and using them:

```
<delta-repo-root>/kernel/examples/run-kernel-examples.py --use-local
```


To run examples using artifacts from a Maven repository:
```
<delta-repo-root>/kernel/examples/run-kernel-examples.py --version <version> --maven-repo <staged_repo_url>
```

'''

import os
import subprocess
from os import path
import shutil
import argparse

def run_single_threaded_examples(version, maven_repo, examples_root_dir, golden_tables_dir):
    main_class = "io.delta.kernel.examples.SingleThreadedTableReader"
    test_cases = [
        f"--table={golden_tables_dir}/data-reader-primitives --columns=as_int,as_long --limit=5",
        f"--table={golden_tables_dir}/data-reader-primitives --columns=as_int,as_long,as_double,as_string --limit=20",
        f"--table={golden_tables_dir}/data-reader-partition-values --columns=as_string,as_byte,as_list_of_records,as_nested_struct --limit=20"
    ]
    project_dir = path.join(examples_root_dir, "kernel-examples")

    run_example(version, maven_repo, project_dir, main_class, test_cases)


def run_multi_threaded_examples(version, maven_repo, examples_root_dir, golden_tables_dir):
    main_class = "io.delta.kernel.examples.MultiThreadedTableReader"
    test_cases = [
        f"--table={golden_tables_dir}/data-reader-primitives --columns=as_int,as_long --limit=5 --parallelism=5",
        f"--table={golden_tables_dir}/data-reader-primitives --columns=as_int,as_long,as_double,as_string --limit=20 --parallelism=20",
        f"--table={golden_tables_dir}/data-reader-partition-values --columns=as_string,as_byte,as_list_of_records,as_nested_struct --limit=20 --parallelism=2"
    ]
    project_dir = path.join(examples_root_dir, "kernel-examples")

    run_example(version, maven_repo, project_dir, main_class, test_cases)


def run_integration_tests(version, maven_repo, examples_root_dir, golden_tables_dir):

    main_classes = ["io.delta.kernel.integration.ReadIntegrationTestSuite", "io.delta.kernel.integration.WriteIntegrationTestSuite"]
    for main_class in main_classes:
        project_dir = path.join(examples_root_dir, "kernel-examples")
        with WorkingDirectory(project_dir):
            cmd = ["mvn", "package", "exec:java", f"-Dexec.mainClass={main_class}",
                  f"-Dstaging.repo.url={maven_repo}",
                  f"-Ddelta-kernel.version={version}",
                  f"-Dexec.args={golden_tables_dir}"]
            run_cmd(cmd, stream_output=True)


def run_example(version, maven_repo, project_dir, main_class, test_cases):
    with WorkingDirectory(project_dir):
        for test in test_cases:
            cmd = ["mvn", "package", "exec:java", f"-Dexec.mainClass={main_class}",
                   f"-Dstaging.repo.url={maven_repo}",
                   f"-Ddelta-kernel.version={version}",
                   f"-Dexec.args={test}"]
            run_cmd(cmd, stream_output=True)


def clear_artifact_cache():
    print("Clearing Delta Kernel artifacts from ivy2 and mvn cache")
    ivy_caches_to_clear = [path for path in os.listdir(os.path.expanduser("~")) if path.startswith(".ivy")]
    print(f"Clearing Ivy caches in: {ivy_caches_to_clear}")
    for path in ivy_caches_to_clear:
        for subpath in ["io.delta", "io.delta.kernel"]:
            delete_if_exists(os.path.expanduser(f"~/{path}/cache/{subpath}"))
            delete_if_exists(os.path.expanduser(f"~/{path}/local/{subpath}"))
    delete_if_exists(os.path.expanduser("~/.m2/repository/io/delta/"))


def delete_if_exists(path):
    # if path exists, delete it.
    if os.path.exists(path):
        shutil.rmtree(path)
        print("Deleted %s " % path)


# pylint: disable=too-few-public-methods
class WorkingDirectory(object):
    def __init__(self, working_directory):
        self.working_directory = working_directory
        self.old_workdir = os.getcwd()

    def __enter__(self):
        os.chdir(self.working_directory)

    def __exit__(self, tpe, value, traceback):
        os.chdir(self.old_workdir)


def run_cmd(cmd, throw_on_error=True, env=None, stream_output=False, **kwargs):
    cmd_env = os.environ.copy()
    if env:
        cmd_env.update(env)

    if stream_output:
        child = subprocess.Popen(cmd, env=cmd_env, **kwargs)
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception("Non-zero exitcode: %s" % (exit_code))
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


if __name__ == "__main__":
    """
        Script to run Delta Kernel examples which are located in the kernel/examples directory.
        call this by running `python run-kernel-examples.py`
        additionally the version can be provided as a command line argument.
    """

    # get the version of the package
    examples_root_dir = path.abspath(path.dirname(__file__))
    project_root_dir = path.join(examples_root_dir, "../../")
    with open(path.join(project_root_dir, "version.sbt")) as fd:
        default_version = fd.readline().split('"')[1]

        parser = argparse.ArgumentParser()
    parser.add_argument(
        "--version",
        required=False,
        default=default_version,
        help="Delta Kernel version to use to run the examples")

    parser.add_argument(
        "--maven-repo",
        required=False,
        default=None,
        help="Additional Maven repo to resolve staged new release artifacts")

    parser.add_argument(
        "--use-local",
        required=False,
        default=False,
        action="store_true",
        help="Generate JARs from local source code and use to run tests")

    args = parser.parse_args()

    if args.use_local and (args.version != default_version):
        raise Exception("Cannot specify --use-local with a --version different than in version.sbt")

    clear_artifact_cache()

    if args.use_local:
        with WorkingDirectory(project_root_dir):
            run_cmd(["build/sbt", "kernelGroup/publishM2", "storage/publishM2"], stream_output=True)

    golden_file_dir = path.join(
        examples_root_dir,
        "../../connectors/golden-tables/src/main/resources/golden/")

    run_single_threaded_examples(args.version, args.maven_repo, examples_root_dir, golden_file_dir)
    run_multi_threaded_examples(args.version, args.maven_repo, examples_root_dir, golden_file_dir)
    run_integration_tests(args.version, args.maven_repo, examples_root_dir, golden_file_dir)
