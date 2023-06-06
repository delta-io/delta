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
    with WorkingDirectory(test_dir):
        cmd = ["mvn", "package", "exec:java", "-Dexec.cleanupDaemonThreads=false",
            f"-Dexec.mainClass=example.{example}",
            f"-Dscala.version={scala_version}", f"-Dstaging.repo.url={maven_repo}",
            f"-Dstandalone.version={version}"]
        run_cmd(cmd, stream_output=True)

def run_sbt_proj(test_dir, proj, className, version, maven_repo, scala_version):
    print(f"\n\n##### Running SBT verification {proj} on standalone version {version} with scala version {scala_version}#####")

    env = {"STANDALONE_VERSION": str(version)}
    if maven_repo:
        env["EXTRA_MAVEN_REPO"] = maven_repo
    with WorkingDirectory(test_dir):
        cmd = ["build/sbt", f"++ {scala_version}", f"{proj}/runMain example.{className}"]
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
    Call this by running "python3 run-examples.py --version <version>", where <version> is the
    Delta Connectors repo version to use.
    
    There are two version 'modes' you should use to run this file.
    1. using published or staged jar: explicitly pass in the --version argument.
    2. using locally-generated jar (e.g. x.y.z-SNAPSHOT): explicitly pass in the --version argument
       and --use-local-cache argument.
       
       In this mode, ensure that the local jar exists for all scala versions. You can generate it
       by running the following commands in the root connectors folder.
       
       build/sbt '++2.11.12 publishM2'
       build/sbt '++2.12.8 publishM2'
       build/sbt '++2.13.8 publishM2'
    """

    # get the version of the package
    root_dir = path.dirname(__file__)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--version",
        required=True,
        help="Delta Standalone version to use to run the integration tests")
    parser.add_argument(
        "--maven-repo",
        required=False,
        default=None,
        help="Additional Maven repo to resolve staged new release artifacts")
    parser.add_argument(
        "--use-local-cache",
        required=False,
        default=False,
        action="store_true",
        help="Don't clear Delta artifacts from ivy2 and mvn cache")

    args = parser.parse_args()

    if not args.use_local_cache:
        clear_artifact_cache()

    examples = [("convert-to-delta", "convertToDelta", "ConvertToDelta"),
                ("hello-world", "helloWorld", "HelloWorld")]

    for dir, proj, className in examples:
        run_maven_proj(path.join(root_dir, dir), className, args.version, args.maven_repo, "2.11")
        run_maven_proj(path.join(root_dir, dir), className, args.version, args.maven_repo, "2.12")
        run_maven_proj(path.join(root_dir, dir), className, args.version, args.maven_repo, "2.13")

        run_sbt_proj(root_dir, proj, className, args.version, args.maven_repo, "2.11.12")
        run_sbt_proj(root_dir, proj, className, args.version, args.maven_repo, "2.12.8")
        run_sbt_proj(root_dir, proj, className, args.version, args.maven_repo, "2.13.8")
