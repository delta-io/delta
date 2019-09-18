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
import fnmatch
import subprocess
from os import path


def test(root_dir, jar_file):
    # And then, run all of the test under test/python directory, each of them
    # has main entry point to execute, which is python's unittest testing
    # framework.
    test_dir = path.join(root_dir,
                         path.join("python", path.join("delta", "tests")))
    test_files = [os.path.join(test_dir, f) for f in os.listdir(test_dir)
                  if os.path.isfile(os.path.join(test_dir, f)) and
                  f.endswith(".py") and not f.startswith("_")]
    for test_file in test_files:
        try:
            subprocess.check_output(["spark-submit", "--jars", jar_file,
                                     "--py-files", jar_file, test_file])
        except Exception as e:
            print(e)
            raise Exception("Failed test %s: " % test_file)


def prepare(root_dir):
    # Build package with python files in it
    sbt_path = path.join(root_dir, path.join("build", "sbt"))
    subprocess.call([sbt_path, "clean", "++ 2.11.12 spPackage"])
    target_dir = path.join(root_dir, "target")
    jar_files = []
    for root, dirnames, filenames in os.walk(target_dir):
        for filename in fnmatch.filter(filenames, '*.jar'):
            jar_files.append(path.join(root, filename))
    if len(jar_files) > 1:
        raise Exception("Multiple JAR files found in %s: %s "
                        % (target_dir, jar_files.join(", ")))
    elif len(jar_files) == 0:
        raise Exception("No JAR file found in %s" % target_dir)
    return jar_files[0]


if __name__ == "__main__":
    root_dir = os.path.dirname(os.path.dirname(__file__))
    jar_file = prepare(root_dir)
    test(root_dir, jar_file)
