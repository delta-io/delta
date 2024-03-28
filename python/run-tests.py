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
import shutil
from os import path


def test(root_dir, package):
    # Run all of the test under test/python directory, each of them
    # has main entry point to execute, which is python's unittest testing
    # framework.
    python_root_dir = path.join(root_dir, "python")
    test_dir = path.join(python_root_dir, path.join("delta", "tests"))
    test_files = [os.path.join(test_dir, f) for f in os.listdir(test_dir)
                  if os.path.isfile(os.path.join(test_dir, f)) and
                  f.endswith(".py") and not f.startswith("_")]
    extra_class_path = path.join(python_root_dir, path.join("delta", "testing"))

    for test_file in test_files:
        try:
            cmd = ["spark-submit",
                   "--driver-class-path=%s" % extra_class_path,
                   "--packages", package, test_file]
            print("Running tests in %s\n=============" % test_file)
            print("Command: %s" % str(cmd))
            run_cmd(cmd, stream_output=True)
        except:
            print("Failed tests in %s" % (test_file))
            raise


def delete_if_exists(path):
    # if path exists, delete it.
    if os.path.exists(path):
        shutil.rmtree(path)
        print("Deleted %s " % path)


def prepare(root_dir):
    print("##### Preparing python tests & building packages #####")
    # Build package with python files in it
    sbt_path = path.join(root_dir, path.join("build", "sbt"))
    delete_if_exists(os.path.expanduser("~/.ivy2/cache/io.delta"))
    delete_if_exists(os.path.expanduser("~/.m2/repository/io/delta/"))
    run_cmd([sbt_path, "clean", "sparkGroup/publishM2"], stream_output=True)

    # Get current release which is required to be loaded
    version = '0.0.0'
    with open(os.path.join(root_dir, "version.sbt")) as fd:
        version = fd.readline().split('"')[1]
    package = "io.delta:delta-spark_2.12:" + version
    return package


def run_cmd(cmd, throw_on_error=True, env=None, stream_output=False, print_cmd=True, **kwargs):
    if print_cmd:
        print("### Executing cmd: " + " ".join(cmd))

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


def run_python_style_checks(root_dir):
    print("##### Running python style tests #####")
    run_cmd([os.path.join(root_dir, "dev", "lint-python")], stream_output=True)


def run_mypy_tests(root_dir):
    print("##### Running mypy tests #####")
    python_package_root = path.join(root_dir, path.join("python", "delta"))
    mypy_config_path = path.join(root_dir, path.join("python", "mypy.ini"))
    run_cmd([
        "mypy",
        "--config-file", mypy_config_path,
        python_package_root
    ], stream_output=True)


def run_pypi_packaging_tests(root_dir):
    """
    We want to test that the delta-spark PyPi artifact for this delta version can be generated,
    locally installed, and used in python tests.

    We will uninstall any existing local delta-spark PyPi artifact.
    We will generate a new local delta-spark PyPi artifact.
    We will install it into the local PyPi repository.
    And then we will run relevant python tests to ensure everything works as expected.
    """
    print("##### Running PyPi Packaging tests #####")

    version = '0.0.0'
    with open(os.path.join(root_dir, "version.sbt")) as fd:
        version = fd.readline().split('"')[1]

    # uninstall packages if they exist
    run_cmd(["pip3", "uninstall", "--yes", "delta-spark"], stream_output=True)

    wheel_dist_dir = path.join(root_dir, "dist")

    print("### Deleting `dist` directory if it exists")
    delete_if_exists(wheel_dist_dir)

    # generate artifacts
    run_cmd(
        ["python3", "setup.py", "bdist_wheel"],
        stream_output=True,
        stderr=open('/dev/null', 'w'))

    run_cmd(["python3", "setup.py", "sdist"], stream_output=True)

    # we need, for example, 1.1.0_SNAPSHOT not 1.1.0-SNAPSHOT
    version_formatted = version.replace("-", "_")
    delta_whl_name = "delta_spark-" + version_formatted + "-py3-none-any.whl"

    # this will install delta-spark-$version
    install_whl_cmd = ["pip3", "install", path.join(wheel_dist_dir, delta_whl_name)]
    run_cmd(install_whl_cmd, stream_output=True)

    # run test python file directly with python and not with spark-submit
    test_file = path.join(root_dir, path.join("examples", "python", "using_with_pip.py"))
    test_cmd = ["python3", test_file]
    try:
        print("### Starting tests...")
        run_cmd(test_cmd, stream_output=True)
    except:
        print("Failed pip installation tests in %s" % (test_file))
        raise


if __name__ == "__main__":
    print("##### Running python tests #####")
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    package = prepare(root_dir)

    run_python_style_checks(root_dir)
    run_mypy_tests(root_dir)
    run_pypi_packaging_tests(root_dir)
    test(root_dir, package)
