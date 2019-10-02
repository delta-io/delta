#!/usr/bin/env python

#
# Copyright 2019 Databricks, Inc.
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
import sys
import fnmatch
import subprocess
from os import path
import random
import string
import tempfile


# TODO Scala integration tests
def run_scala_integration_tests(root_dir, package, repo):
    pass


def run_python_integration_tests(root_dir, package, repo):
    print("##### Running Python tests #####")
    test_dir = path.join(root_dir, path.join("examples", "python"))
    test_files = [os.path.join(test_dir, f) for f in os.listdir(test_dir)
                  if os.path.isfile(os.path.join(test_dir, f)) and
                  f.endswith(".py") and not f.startswith("_")]
    python_root_dir = path.join(root_dir, "python")
    extra_class_path = path.join(python_root_dir, path.join("delta", "testing"))
    cmd = ["spark-submit",
           "--driver-class-path=%s" % extra_class_path,
           "--packages", package]
    if repo != None:
        cmd.append("--repositories")
        cmd.append(repo)

    for test_file in test_files:
        try:
            cmd.append(test_file)
            print("Running tests in %s\n=============" % test_file)
            print("Command: %s" % str(cmd))
            run_cmd(cmd, stream_output=True)
        except:
            print("Failed tests in %s" % (test_file))
            raise


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
        if throw_on_error and exit_code is not 0:
            raise Exception(
                "Non-zero exitcode: %s\n\nSTDOUT:\n%s\n\nSTDERR:%s" %
                (exit_code, stdout, stderr))
        return (exit_code, stdout, stderr)


if __name__ == "__main__":
    """
        Script to run integration tests which are located in the examples directory.
        call this by running "python run-integration-tests.py"
        an additional maven repo url can be provided as an argument.
        "
    """
    root_dir = os.path.dirname(os.path.dirname(__file__))
    repo = None
    # check if repo is provided as an argument
    if len(sys.argv) >= 2:
        repo = sys.argv[1]

    # get the version of the package
    version = '0.0.0'
    with open(os.path.join(root_dir, "version.sbt")) as fd:
        version = fd.readline().split('"')[1]
    package = "io.delta:delta-core_2.11:" + version

    run_scala_integration_tests(root_dir, package, repo)
    run_python_integration_tests(root_dir, package, repo)
