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
import csv
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location(
    "collect_test_durations",
    Path(__file__).resolve().parents[1] / "scripts" / "collect_test_durations.py")
collector = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(collector)


class CollectTestDurationsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.output = contextlib.ExitStack()
        self.addCleanup(self.output.close)
        self.output.enter_context(contextlib.redirect_stdout(io.StringIO()))
        self.output.enter_context(contextlib.redirect_stderr(io.StringIO()))

    def write_report(self, text):
        (self.directory / "TEST-suite.xml").write_text(text)

    def artifacts(self, version):
        return [{"id": shard, "spark_version": version, "shard": shard}
                for shard in range(collector.SHARDS_PER_SPARK_VERSION)]

    def test_queries_completed_workflows(self):
        runs = [{"databaseId": 10, "headSha": "abcdef0123", "createdAt": "2026-10-15"}]
        with mock.patch.object(collector, "run_gh", return_value=json.dumps(runs)) as run:
            self.assertEqual(collector.get_run_ids(3), [10])
        args = run.call_args[0][0]
        self.assertEqual(args[args.index("--status") + 1], "completed")

    def test_ignores_expired_and_unrelated_artifacts(self):
        artifacts = [
            {"id": 1, "name": "test-reports-spark4.1-shard0", "expired": False},
            {"id": 2, "name": "test-reports-spark4.1-shard1", "expired": True},
            {"id": 3, "name": "test-reports-spark4.1-shard0-extra", "expired": False},
            {"id": 4, "name": "other-report", "expired": False},
        ]
        with mock.patch.object(collector, "run_gh", return_value="\n".join(
                json.dumps(artifact) for artifact in artifacts)):
            self.assertEqual(collector.list_artifacts(10),
                             [{"id": 1, "spark_version": "4.1", "shard": 0}])

    def test_reads_published_and_source_spark_jobs(self):
        jobs = [
            {"name": "DS: Spark 4.1, Scala 2.13.16, Shard 0", "conclusion": "success"},
            {"name": "DS Source Spark 4.2, Scala 2.13.16, Shard 1",
             "conclusion": "success"},
            {"name": "DS: Spark 4.1, Scala 2.13.16, Shard 2", "conclusion": "failure"},
            {"name": "Generate Spark Versions Matrix", "conclusion": "success"},
        ]
        with mock.patch.object(collector, "run_gh", return_value="\n".join(
                json.dumps(job) for job in jobs)):
            self.assertEqual(dict(collector.get_successful_shards(10)),
                             {"4.1": {0}, "4.2": {1}})

    def test_download_failure_rejects_the_artifact(self):
        with mock.patch.object(collector.subprocess, "run", return_value=mock.Mock(returncode=1)):
            with self.assertRaises(ValueError):
                collector.download_and_extract(1, self.directory)

    def test_parses_nested_reports_and_keeps_maximum(self):
        self.write_report('<testsuites><testsuite name="a" time="60"/>'
                          '<testsuite name="a" time="120"/>'
                          '<testsuite name="b" time="0"/></testsuites>')
        self.assertEqual(collector.parse_junit_xmls(self.directory), {"a": 2, "b": 0})

    def test_rejects_bad_reports(self):
        for report in [
                "not XML", "<testsuites/>",
                '<testsuite name="a" time="60" failures="1"/>',
                '<testsuite name="a" time="60" errors="1"/>',
                '<testsuite name="a" time="NaN"/>',
                '<testsuite name="a" time="inf"/>',
                '<testsuite name="a" time="-1"/>',
                '<testsuite name="a" time="invalid"/>']:
            with self.subTest(report=report):
                self.write_report(report)
                with self.assertRaises(ValueError):
                    collector.parse_junit_xmls(self.directory)

    def test_all_skipped_suite_has_no_test_time(self):
        cases = '<testcase time="-0.001"><skipped/></testcase>' * 4
        self.write_report('<testsuite name="ignored" time="-0.004">' + cases + '</testsuite>')
        self.assertEqual(collector.parse_junit_xmls(self.directory), {"ignored": 0})

    def test_skipped_placeholders_do_not_hide_executed_test_time(self):
        cases = '<testcase time="0.6"/>' + '<testcase time="-0.001"><skipped/></testcase>' * 601
        self.write_report('<testsuite name="mixed" time="-0.001">' + cases + '</testsuite>')
        self.assertEqual(collector.parse_junit_xmls(self.directory), {"mixed": 0.01})

    def test_collects_only_complete_successful_versions(self):
        artifacts = self.artifacts("4.1") + self.artifacts("4.2")
        successful = {"4.1": set(range(8)), "4.2": set(range(7))}
        with mock.patch.object(collector, "list_artifacts", return_value=artifacts), \
                mock.patch.object(collector, "get_successful_shards", return_value=successful), \
                mock.patch.object(collector, "download_and_extract", return_value=self.directory) \
                as download, \
                mock.patch.object(collector, "parse_junit_xmls", return_value={"suite": 2}):
            self.assertEqual(collector.collect_from_run(10, self.directory), {"suite": 2})
            self.assertEqual(download.call_count, 8)

    def test_rejects_missing_and_duplicate_artifacts(self):
        complete = self.artifacts("4.1")
        for artifacts in [complete[:-1], complete + [complete[0]]]:
            with self.subTest(artifacts=artifacts), \
                    mock.patch.object(collector, "list_artifacts", return_value=artifacts), \
                    mock.patch.object(collector, "get_successful_shards",
                                      return_value={"4.1": set(range(8))}), \
                    mock.patch.object(collector, "download_and_extract") as download:
                self.assertEqual(collector.collect_from_run(10, self.directory), {})
                download.assert_not_called()

    def test_bad_shard_discards_the_whole_version(self):
        with mock.patch.object(collector, "list_artifacts", return_value=self.artifacts("4.1")), \
                mock.patch.object(collector, "get_successful_shards",
                                  return_value={"4.1": set(range(8))}), \
                mock.patch.object(collector, "download_and_extract", return_value=self.directory), \
                mock.patch.object(collector, "parse_junit_xmls",
                                  side_effect=[{"suite": 2}, ValueError("bad report")]):
            self.assertEqual(collector.collect_from_run(10, self.directory), {})

    def test_time_limit_discards_an_incomplete_version(self):
        with mock.patch.object(collector, "list_artifacts", return_value=self.artifacts("4.1")), \
                mock.patch.object(collector, "get_successful_shards",
                                  return_value={"4.1": set(range(8))}), \
                mock.patch.object(collector, "download_and_extract", return_value=self.directory), \
                mock.patch.object(collector, "parse_junit_xmls", return_value={"suite": 2}), \
                mock.patch.object(collector.time, "monotonic", side_effect=[0, 2]):
            self.assertEqual(collector.collect_from_run(10, self.directory, deadline=1), {})

    def test_default_output_keeps_all_suites_and_stable_ties(self):
        durations = {"suite{:03d}".format(i): 1 for i in reversed(range(300))}
        csv_file = self.directory / "durations.csv"
        with mock.patch.object(collector, "CSV_FILE", str(csv_file)), \
                mock.patch.object(collector, "get_run_ids", return_value=[10]), \
                mock.patch.object(collector, "collect_from_run", return_value=durations), \
                mock.patch("sys.argv", ["collect_test_durations.py", "--max-minutes", "0"]):
            collector.main()
        with csv_file.open() as source:
            rows = list(csv.DictReader(source))
        self.assertEqual([row["suite_name"] for row in rows], sorted(durations))


if __name__ == "__main__":
    unittest.main()
