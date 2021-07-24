#!/usr/bin/env python

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
    # Build package with python files in it
    sbt_path = path.join(root_dir, path.join("build", "sbt"))
    delete_if_exists(os.path.expanduser("~/.ivy2/cache/io.delta"))
    delete_if_exists(os.path.expanduser("~/.m2/repository/io/delta/"))
    run_cmd([sbt_path, "clean", "publishM2"], stream_output=True)

    # Get current release which is required to be loaded
    version = '0.0.0'
    with open(os.path.join(root_dir, "version.sbt")) as fd:
        version = fd.readline().split('"')[1]
    package = "io.delta:delta-core_2.12:" + version
    return package


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


def run_python_style_checks(root_dir):
    run_cmd([os.path.join(root_dir, "dev", "lint-python")], stream_output=True)

def run_pypi_packaging_tests(root_dir):
    print("##### Running PyPi Packaging tests #####")
    version = '0.0.0'
    with open(os.path.join(root_dir, "version.sbt")) as fd:
        version = fd.readline().split('"')[1]

    # uninstall packages if they exist
    run_cmd(["pip3", "uninstall", "--yes", "delta-spark", "pyspark"], stream_output=True)

    print("### Clearing Delta artifacts from ivy2 and mvn cache")
    run_cmd(["rm", "-rf", "~/.ivy2/cache/io.delta/"], stream_output=True)
    run_cmd(["rm", "-rf", "~/.m2/repository/io/delta/"], stream_output=True)
    
    install_cmd = ["pip3", "install", "wheel", "twine", "setuptools", "--upgrade"]
    print("### Executing:" + " ".join(install_cmd))
    run_cmd(install_cmd, stream_output=True)

    dist_dir = path.join(root_dir, "dist")

    print("### Deleting `dist` directory if it exists")
    delete_if_exists(dist_dir)

    gen_artifacts_cmd_1 = ["python3", "setup.py", "bdist_wheel"]
    print("### Executing: " + " ".join(gen_artifacts_cmd_1))
    run_cmd(gen_artifacts_cmd_1, stream_output=True, stderr=open('/dev/null', 'w'))

    gen_artifacts_cmd_2 = ["python3", "setup.py", "sdist"]
    print("### Executing: " + " ".join(gen_artifacts_cmd_2))
    run_cmd(gen_artifacts_cmd_2, stream_output=True)

        # we need, for example, 1.1.0_SNAPSHOT not 1.1.0-SNAPSHOT
    version_formatted = version.replace("-","_")
    delta_whl_name = "delta_spark-" + version_formatted + "-py3-none-any.whl"

    install_whl_cmd = ["pip3", "install", path.join(dist_dir, delta_whl_name)]
    print("### Executing: " + " ".join(install_whl_cmd))
    run_cmd(install_whl_cmd, stream_output=True)

    # run test python file directly with python and not with spark-submit
    test_file = path.join(root_dir, path.join("examples", "python", "using_with_pip.py"))
    test_cmd = ["python3", test_file]
    print("Test command: %s" % str(test_cmd))
    try:
        run_cmd(test_cmd, stream_output=True)
    except:
        print("Failed pip installation tests in %s" % (test_file))
        raise


if __name__ == "__main__":
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    package = prepare(root_dir)
    # run_python_style_checks(root_dir)
    # test(root_dir, package)
    run_pypi_packaging_tests(root_dir)
