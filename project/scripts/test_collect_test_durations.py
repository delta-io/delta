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

import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import collect_test_durations as collector


class CollectTestDurationsTest(unittest.TestCase):
    def test_pr_history_and_master_fallback(self):
        run = dict(databaseId=7, headSha="0123456789", createdAt="2026-01-01T00:00:00Z")
        for branch, histories, expected_branches in [
                ("feature", [[run]], ["feature"]),
                ("feature", [[], [run]], ["feature", "master"]),
                ("", [[run]], ["master"])]:
            with self.subTest(branch=branch, histories=histories):
                with patch.dict("os.environ", {"GITHUB_HEAD_REF": branch}), \
                        patch.object(collector, "run_gh", side_effect=[
                            json.dumps(history) for history in histories]) as query:
                    self.assertEqual(collector.get_run_ids(5), [7])
                self.assertEqual([
                    call.args[0][call.args[0].index("--branch") + 1]
                    for call in query.call_args_list], expected_branches)

    def test_parallel_reports_keep_the_longest_suite_duration(self):
        with tempfile.TemporaryDirectory() as temporary:
            artifacts = [dict(id=i, spark_version="1.0", shard=i) for i in range(4)]
            for artifact in artifacts:
                directory = Path(temporary) / str(artifact["id"])
                directory.mkdir()
                (directory / "report.xml").write_text(
                    '<testsuite name="shared" time="{}"/>'.format(
                        60 * (artifact["id"] + 1)))
            downloads = threading.Barrier(len(artifacts), timeout=5)

            def download(artifact_id, destination):
                downloads.wait()
                return str(Path(destination) / str(artifact_id))

            with patch.object(collector, "list_artifacts", return_value=artifacts), \
                    patch.object(collector, "download_and_extract", side_effect=download):
                self.assertEqual(collector.collect_from_run(1, temporary), {"shared": 4.0})

    def test_failed_download_preserves_the_timing_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            timing_file = Path(temporary) / "durations.csv"
            fallback = "suite_name,duration_minutes\nshared,5.0\n"
            timing_file.write_text(fallback)
            artifact = dict(id=1, spark_version="1.0", shard=0)
            with patch.object(collector, "CSV_FILE", str(timing_file)), \
                    patch.object(collector, "get_run_ids", return_value=[1]), \
                    patch.object(collector, "list_artifacts", return_value=[artifact]), \
                    patch.object(collector, "download_and_extract", side_effect=SystemExit(1)), \
                    patch("sys.argv", ["collect_test_durations.py"]):
                with self.assertRaises(SystemExit) as failure:
                    collector.main()
            self.assertEqual(failure.exception.code, 1)
            self.assertEqual(timing_file.read_text(), fallback)


if __name__ == "__main__":
    unittest.main()
