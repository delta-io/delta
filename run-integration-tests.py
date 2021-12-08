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


def run_scala_integration_tests(root_dir, version, test_name, extra_maven_repo):
    print("\n\n##### Running Scala tests on version %s #####" % str(version))
    clear_artifact_cache()
    test_dir = path.join(root_dir, "examples", "scala")
    test_src_dir = path.join(test_dir, "src", "main", "scala", "example")
    test_classes = [f.replace(".scala", "") for f in os.listdir(test_src_dir)
                    if f.endswith(".scala") and not f.startswith("_")]
    env = {"DELTA_VERSION": str(version)}
    if extra_maven_repo:
        env["EXTRA_MAVEN_REPO"] = extra_maven_repo
    with WorkingDirectory(test_dir):
        for test_class in test_classes:
            if test_name is not None and test_name not in test_class:
                print("\nSkipping Scala tests in %s\n=====================" % test_class)
                continue

            try:
                cmd = ["build/sbt", "runMain example.%s" % test_class]
                print("\nRunning Scala tests in %s\n=====================" % test_class)
                print("Command: %s" % str(cmd))
                run_cmd(cmd, stream_output=True, env=env)
            except:
                print("Failed Scala tests in %s" % (test_class))
                raise


def run_python_integration_tests(root_dir, version, test_name, extra_maven_repo):
    print("\n\n##### Running Python tests on version %s #####" % str(version))
    clear_artifact_cache()
    test_dir = path.join(root_dir, path.join("examples", "python"))
    files_to_skip = {"using_with_pip.py"}

    test_files = [path.join(test_dir, f) for f in os.listdir(test_dir)
                  if path.isfile(path.join(test_dir, f)) and
                  f.endswith(".py") and not f.startswith("_") and
                  f not in files_to_skip]

    python_root_dir = path.join(root_dir, "python")
    extra_class_path = path.join(python_root_dir, path.join("delta", "testing"))
    package = "io.delta:delta-core_2.12:" + version

    if extra_maven_repo:
        repo = extra_maven_repo
    else:
        repo = ""
    for test_file in test_files:
        if test_name is not None and test_name not in test_file:
            print("\nSkipping Python tests in %s\n=====================" % test_file)
            continue

        try:
            cmd = ["spark-submit",
                   "--driver-class-path=%s" % extra_class_path,  # for less verbose logging
                   "--packages", package,
                   "--repositories", repo, test_file]
            print("\nRunning Python tests in %s\n=============" % test_file)
            print("Command: %s" % str(cmd))
            run_cmd(cmd, stream_output=True)
        except:
            print("Failed Python tests in %s" % (test_file))
            raise


def run_pip_installation_tests(root_dir, version, use_testpypi, extra_maven_repo):
    print("\n\n##### Running pip installation tests on version %s #####" % str(version))
    clear_artifact_cache()
    delta_pip_name = "delta-spark"
    # uninstall packages if they exist
    run_cmd(["pip", "uninstall", "--yes", delta_pip_name, "pyspark"], stream_output=True)

    # install packages
    delta_pip_name_with_version = "%s==%s" % (delta_pip_name, str(version))
    if use_testpypi:
        install_cmd = ["pip", "install",
                       "--extra-index-url", "https://test.pypi.org/simple/",
                       delta_pip_name_with_version]
    else:
        install_cmd = ["pip", "install", delta_pip_name_with_version]
    print("pip install command: %s" % str(install_cmd))
    run_cmd(install_cmd, stream_output=True)

    # run test python file directly with python and not with spark-submit
    env = {}
    if extra_maven_repo:
        env["EXTRA_MAVEN_REPO"] = extra_maven_repo
    test_file = path.join(root_dir, path.join("examples", "python", "using_with_pip.py"))
    test_cmd = ["python3", test_file]
    print("Test command: %s" % str(test_cmd))
    try:
        run_cmd(test_cmd, stream_output=True, env=env)
    except:
        print("Failed pip installation tests in %s" % (test_file))
        raise


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
    """
        Script to run integration tests which are located in the examples directory.
        call this by running "python run-integration-tests.py"
        additionally the version can be provided as a command line argument.
        "
    """

    # get the version of the package
    root_dir = path.dirname(__file__)
    with open(path.join(root_dir, "version.sbt")) as fd:
        default_version = fd.readline().split('"')[1]

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--version",
        required=False,
        default=default_version,
        help="Delta version to use to run the integration tests")
    parser.add_argument(
        "--python-only",
        required=False,
        default=False,
        action="store_true",
        help="Run only Python tests")
    parser.add_argument(
        "--scala-only",
        required=False,
        default=False,
        action="store_true",
        help="Run only Scala tests")
    parser.add_argument(
        "--pip-only",
        required=False,
        default=False,
        action="store_true",
        help="Run only pip installation tests")
    parser.add_argument(
        "--no-pip",
        required=False,
        default=False,
        action="store_true",
        help="Do not run pip installation tests")
    parser.add_argument(
        "--test",
        required=False,
        default=None,
        help="Run a specific test by substring-match with Scala/Python file name")
    parser.add_argument(
        "--maven-repo",
        required=False,
        default=None,
        help="Additional Maven repo to resolve staged new release artifacts")
    parser.add_argument(
        "--use-testpypi",
        required=False,
        default=False,
        action="store_true",
        help="Use testpypi for testing pip installation")

    args = parser.parse_args()

    if args.pip_only and args.no_pip:
        raise Exception("Cannot specify both --pip-only and --no-pip")

    run_python = not args.scala_only and not args.pip_only
    run_scala = not args.python_only and not args.pip_only
    run_pip = not args.python_only and not args.scala_only and not args.no_pip

    if run_scala:
        run_scala_integration_tests(root_dir, args.version, args.test, args.maven_repo)

    if run_python:
        run_python_integration_tests(root_dir, args.version, args.test, args.maven_repo)

    if run_pip:
        run_pip_installation_tests(root_dir, args.version, args.use_testpypi, args.maven_repo)
