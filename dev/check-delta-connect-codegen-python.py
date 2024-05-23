#!/usr/bin/env python3

#
# Copyright (2024) The Delta Lake Project Authors.
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

# Utility for checking whether generated the Delta Connect Python protobuf codes are out of sync.
#   usage: ./dev/check-delta-connect-codegen-python.py

import os
import sys
import filecmp
import tempfile
import subprocess

# Location of your Spark git development area
DELTA_HOME = os.environ.get("DELTA_HOME", os.getcwd())


def fail(msg):
    print(msg)
    sys.exit(-1)


def run_cmd(cmd):
    print(f"RUN: {cmd}")
    if isinstance(cmd, list):
        return subprocess.check_output(cmd).decode("utf-8")
    else:
        return subprocess.check_output(cmd.split(" ")).decode("utf-8")


def check_connect_protos():
    print(f"Start checking the generated codes in {DELTA_HOME}/python/delta/connect/proto/.")
    with tempfile.TemporaryDirectory() as tmp:
        run_cmd(f"{DELTA_HOME}/dev/delta-connect-gen-protos.sh {tmp}")
        result = filecmp.dircmp(
            f"{DELTA_HOME}/python/delta/connect/proto/",
            tmp,
            ignore=["__init__.py", "__pycache__"],
        )
        success = True

        if len(result.left_only) > 0:
            print(f"Unexpected files: {result.left_only}")
            success = False

        if len(result.right_only) > 0:
            print(f"Missing files: {result.right_only}")
            success = False

        if len(result.funny_files) > 0:
            print(f"Incomparable files: {result.funny_files}")
            success = False

        if len(result.diff_files) > 0:
            print(f"Different files: {result.diff_files}")
            success = False

        if success:
            print(f"Finish checking the generated codes in {DELTA_HOME}/python/delta/connect/proto/: SUCCESS")
        else:
            fail(
                f"Generated files for {DELTA_HOME}/python/delta/connect/proto/ are out of sync! "
                "Please run ./dev/delta-connect-gen-protos.sh"
            )


check_connect_protos()
