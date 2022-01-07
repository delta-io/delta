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

import os
import subprocess
from os import path
import shutil
import argparse


def delete_if_exists(path):
    # if path exists, delete it.
    if os.path.exists(path):
        shutil.rmtree(path)
        print("Deleted %s " % path)

def run_maven_proj(test_dir, example, version, maven_repo, scala_version):
    print(f"\n\n##### Running Maven verification {example} on standalone version {version} with scala version {scala_version}#####")
    clear_artifact_cache()
    with WorkingDirectory(test_dir):
        cmd = ["mvn", "package", "exec:java", "-Dexec.cleanupDaemonThreads=false",
            f"-Dexec.mainClass=example.{example}",
            f"-Dscala.version={scala_version}", f"-Dstaging.repo.url={maven_repo}",
            f"-Dstandalone.version={version}"]
        run_cmd(cmd, stream_output=True)

def run_sbt_proj(test_dir, example, version, maven_repo, scala_version):
    print(f"\n\n##### Running SBT verification {example} on standalone version {version} with scala version {scala_version}#####")
    clear_artifact_cache()
    env = {"STANDALONE_VERSION": str(version)}
    if maven_repo:
        env["EXTRA_MAVEN_REPO"] = maven_repo
    with WorkingDirectory(test_dir):
        cmd = ["build/sbt", f"++ {scala_version}", f"{example[0].lower() + example[1:]}/runMain example.{example}"]
        run_cmd(cmd, stream_output=True, env=env)

def clear_artifact_cache():
    print("Clearing Delta artifacts from ivy2 and mvn cache")
    delete_if_exists(os.path.expanduser("~/.ivy2/cache/io.delta"))
    delete_if_exists(os.path.expanduser("~/.ivy2/local/io.delta"))
    delete_if_exists(os.path.expanduser("~/.m2/repository/io/delta/"))

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


class WorkingDirectory(object):
    def __init__(self, working_directory):
        self.working_directory = working_directory
        self.old_workdir = os.getcwd()

    def __enter__(self):
        os.chdir(self.working_directory)

    def __exit__(self, tpe, value, traceback):
        os.chdir(self.old_workdir)


if __name__ == "__main__":
    """
    Script to run integration tests which are located in the examples directory.
    call this by running "python3 run-integration-tests.py"
    additionally the version can be provided as a command line argument.
    """

    root_dir = path.dirname(__file__)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--version",
        required=False,
        default="0.3.0",
        help="Delta Standalone version to use to run the integration tests")
    parser.add_argument(
        "--maven-repo",
        required=False,
        default=None,
        help="Additional Maven repo to resolve staged new release artifacts")

    args = parser.parse_args()

    examples = [("convert-to-delta", "ConvertToDelta"),
                ("hello-world", "HelloWorld")]

    for dir, c in examples:
        run_maven_proj(path.join(root_dir, dir), c, args.version, args.maven_repo, "2.11")
        run_maven_proj(path.join(root_dir, dir), c, args.version, args.maven_repo, "2.12")

        run_sbt_proj(root_dir, c, args.version, args.maven_repo, "2.11.12")
        run_sbt_proj(root_dir, c, args.version, args.maven_repo, "2.12.8")
