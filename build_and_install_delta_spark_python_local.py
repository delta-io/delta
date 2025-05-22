#!/usr/bin/env python3
#
# Copyright (2025) The Delta Lake Project Authors.
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


def delete_if_exists(path):
    if os.path.exists(path):
        shutil.rmtree(path)
        print("Deleted %s " % path)


def clear_artifact_cache():
    print("Clearing Delta artifacts from ivy2 and mvn cache")
    delete_if_exists(os.path.expanduser("~/.ivy2/cache/io.delta"))
    delete_if_exists(os.path.expanduser("~/.ivy2/local/io.delta"))
    delete_if_exists(os.path.expanduser("~/.m2/repository/io/delta/"))


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


if __name__ == "__main__":

    # Clear existing maven artifacts & uninstall pypi packages
    print("##### Clearing existing maven artifacts & uninstalling pypi packages #####")
    clear_artifact_cache()
    run_cmd(["pip", "uninstall", "--yes", "delta-spark", "pyspark"], stream_output=True)

    # Get the current version
    # TODO - update your local version to something compatible such as 4.0.0.dev2
    root_dir = path.dirname(__file__)
    with open(path.join(root_dir, "version.sbt")) as fd:
        version = fd.readline().split('"')[1]
    assert not "SNAPSHOT" in version, "PyPI does not allow versions containing SNAPSHOT"

    # Publish maven artifacts to local maven cache
    print("##### Publish maven artifacts to local maven cache #####")
    run_cmd(["build/sbt", "clean", "publishM2"])

    # Build pypi artifacts
    print("##### Building pypi packages #####")
    wheel_dist_dir = path.join(root_dir, "dist")
    delete_if_exists(wheel_dist_dir)
    run_cmd(
        ["python3", "setup.py", "bdist_wheel"],
        stream_output=True)
    run_cmd(["python3", "setup.py", "sdist"], stream_output=True)

    # Install pypi artifacts
    print("##### Installing pypi packages #####")
    delta_whl_name = "delta_spark-" + version + "-py3-none-any.whl"
    install_whl_cmd = ["pip3", "install", path.join(wheel_dist_dir, delta_whl_name)]
    run_cmd(install_whl_cmd, stream_output=True)

    print("##### Installed delta-spark and pyspark successfully #####")