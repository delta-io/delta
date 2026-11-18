#!/usr/bin/env python3

#
# Copyright (2026) The Delta Lake Project Authors.
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

import contextlib
import io
import os
from pathlib import Path
import runpy
import subprocess
import sys
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "run-tests.py"


class RunTestsTest(unittest.TestCase):
    def setUp(self):
        process_patch = patch.object(subprocess, "Popen")
        self.processes = process_patch.start()
        self.addCleanup(process_patch.stop)
        self.processes.return_value.wait.return_value = 0
        self.processes.return_value.communicate.return_value = (b"testhash Dockerfile\n", b"")

    def run_cli(self, args, env=None):
        with patch.dict(os.environ, env or {}, clear=True), \
                patch.object(sys, "argv", [str(SCRIPT)] + args), \
                contextlib.redirect_stdout(io.StringIO()):
            runpy.run_path(str(SCRIPT), run_name="__main__")

    def test_default_cli_cleans_before_running_tests(self):
        self.run_cli([])

        self.processes.assert_called_once()
        command = self.processes.call_args.args[0]
        self.assertEqual(command, [
            str(ROOT / "build" / "sbt"), "clean", "+ test",
            "-v", "-J-XX:+UseG1GC", "-J-Xmx6G",
        ])

    def test_no_clean_preserves_test_selection_and_coverage(self):
        self.run_cli([
            "--no-clean", "--group", "spark", "--coverage", "--shard", "0",
            "--spark-version", "4.0", "--kernel-version", "4.0.0",
        ], env={"SCALA_VERSION": "2.13.16"})

        self.processes.assert_called_once()
        command = self.processes.call_args.args[0]
        self.assertEqual(command, [
            str(ROOT / "build" / "sbt"), "-DsparkVersion=4.0", "-DkernelVersion=4.0.0",
            "coverage", "++ 2.13.16", "sparkGroup/test", "coverageAggregate", "coverageOff",
            "-v", "-J-XX:+UseG1GC", "-J-Xmx6G",
        ])
        self.assertEqual(self.processes.call_args.kwargs["env"]["SHARD_ID"], "0")

    def test_no_clean_rejects_python_tests_before_starting_a_process(self):
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit) as error:
            self.run_cli(["--group", "spark-python", "--no-clean"])

        self.assertEqual(error.exception.code, 2)
        self.assertIn("--no-clean applies only to Scala/Java tests", stderr.getvalue())
        self.processes.assert_not_called()

    def test_docker_forwards_no_clean_only_when_requested(self):
        for clean_args in ([], ["--no-clean"]):
            with self.subTest(clean_args=clean_args):
                self.processes.reset_mock()
                self.run_cli([
                    "--group", "spark", "--kernel-version", "4.0.0",
                ] + clean_args, env={"USE_DOCKER": "1"})

                command = self.processes.call_args.args[0]
                self.assertEqual(command[:3], ["docker", "run", "--rm"])
                script_index = command.index("./run-tests.py")
                self.assertEqual(command[script_index + 1:], [
                    "--group", "spark", "--kernel-version", "4.0.0",
                ] + clean_args)


if __name__ == "__main__":
    unittest.main()
