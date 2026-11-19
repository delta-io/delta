#!/usr/bin/env python3
"""
Unit tests for project/scripts/spark_ci_matrix.py.

Usage:
    python3 -m unittest discover -s project/tests -p 'test_spark_ci_matrix.py' -v
"""

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import spark_ci_matrix as m  # noqa: E402

# The currently supported Spark versions. FOUR_VERSIONS adds a hypothetical newer one.
VERSIONS = ["4.1", "4.2", "4.3"]
FOUR_VERSIONS = ["4.1", "4.2", "4.3", "4.4"]
SRC = "spark/src/main/scala/org/apache/spark/sql/delta/DeltaLog.scala"


def files(*paths):
    return [[p] for p in paths]


class FullMatrixTriggerTest(unittest.TestCase):
    TRIGGERS = [
        "build.sbt",
        "version.sbt",
        ".sbtopts",
        ".jvmopts",
        "project/CrossSparkVersions.scala",
        "project/plugins.sbt",
        "project/build.properties",
        "project/scripts/get_spark_version_info.py",
        "project/scripts/setup_unitycatalog_main.sh",
        "project/scripts/spark_ci_matrix.py",
        "build/sbt",
        "build/sbt-config/repositories",
        "build/sbt-launch-lib.bash",
        "spark/src/main/scala-shims/spark-4.1/Shim.scala",
        "spark/src/main/scala-shims/spark-4.1-4.2/DeltaSqlParserShims.scala",
        "spark-unified/src/test/scala-shims/spark-4.2-4.3/a/b/Suite.java",
        "sharing/src/test/scala-shims/spark-4.1-4.3/SharingStreamingTestShims.scala",
        "spark/src/test/scala-shims/spark-4.3/Shim.scala",
        "spark/v2/src/main/java-shims/spark-4.1/Shim.java",
        "spark/src/main/scala-spark-4.1/Old.scala",
        "spark/src/main/java-spark-master/Old.java",
        "spark/src/main/scala-spark-master/Old.scala",
        "spark/src/main/spark-master/Old.scala",
        ".github/workflows/spark_test.yaml",
        ".github/actions/run-delta-spark-tests/action.yml",
        ".github/actions/setup-unitycatalog/action.yml",
        "run-tests.py",
        "./run-tests.py",
        "setup.py",
        "pyproject.toml",
        "dev/pyproject.toml",
        "python/requirements.txt",
        "python/delta/requirements-dev.txt",
        "python/environment.yml",
        ".github/ci-requirements/spark-python/spark4.1.in",
        "dev/requirements.lock",
        "docker-build-requirements.lock",
        ".github/ci-requirements/spark-python/spark4.1.lock",
        "docs/pnpm-lock.yaml",
        "web/package-lock.json",
        "web/yarn-lock.yml",
        "uv.lock",
        "benchmarks/infrastructure/aws/terraform/.terraform.lock.hcl",
    ]

    NON_TRIGGERS = [
        SRC,
        "spark/src/test/scala/org/apache/spark/sql/delta/DeltaSuite.scala",
        "kernel/kernel-api/src/main/java/io/delta/kernel/internal/util/Clock.java",
        "spark/src/test/scala/org/apache/spark/sql/delta/BlockWritesLocalFileSystem.scala",
        "python/delta/tables.py",
        "docs/environment.yml",
        "dev/requirements.txt",
        ".github/workflows/kernel_test.yaml",
        "README.md",
        "projects.txt",
        "builder/foo.scala",
        "projectx/foo.scala",
        "spark/project/notes.scala",
        "examples/scala/build.sbt",
        "benchmarks/build.sbt",
        "kernel/version.sbt",
        "scalastyle-config.xml",
        ".scalafmt.conf",
        "dev/kernel-checkstyle.xml",
        "dev/spark_structured_logging_style.py",
        "run-integration-tests.py",
        "Dockerfile",
        "docker-build-requirements.in",
        "spark-shared-tests/src/test/scala-shared/Shared.scala",
        "connectors/golden-tables/src/main/resources/golden/x/_delta_log/0.json",
        "spark/src/main/scala/lock.scala",
        "spark/src/main/scala-shimsx/Foo.scala",
        "spark/src/main/scala/scala-shims-notes.txt",
        # Only directory segments match shim patterns, not file names.
        "spark/src/main/scala/scala-shims",
        "",
    ]

    def test_triggers(self):
        for path in self.TRIGGERS:
            with self.subTest(path=path):
                self.assertTrue(m.full_matrix_trigger(path))

    def test_non_triggers(self):
        for path in self.NON_TRIGGERS:
            with self.subTest(path=path):
                self.assertFalse(m.full_matrix_trigger(path))


class GlobTest(unittest.TestCase):
    def test_glob_semantics(self):
        cases = [
            ("a/*.lock", "a/x.lock", True),
            ("a/*.lock", "a/b/x.lock", False),
            ("**/x", "x", True),
            ("**/x", "a/b/x", True),
            ("a/**", "a/b/c", True),
            ("a/**", "ab/c", False),
            ("a?c", "abc", True),
            ("a?c", "a/c", False),
            ("a.b", "aXb", False),
        ]
        for pattern, path, expected in cases:
            with self.subTest(pattern=pattern, path=path):
                regex = "^" + m._glob_to_regex(pattern) + "$"
                self.assertEqual(bool(m.re.match(regex, path)), expected)

    def test_every_pattern_matches_something(self):
        # Guards against typos that make a pattern dead.
        examples = {
            p: p.replace("**/", "x/").replace("**", "y/z").replace("*", "w")
            for p in m.FULL_MATRIX_PATTERNS
        }
        for pattern, example in examples.items():
            with self.subTest(pattern=pattern):
                self.assertTrue(m.full_matrix_trigger(example), example)


class SplitVersionsTest(unittest.TestCase):
    def test_one_version(self):
        self.assertEqual(m.split_versions(["4.1"]), (["4.1"], []))

    def test_two_versions(self):
        self.assertEqual(m.split_versions(["4.3", "4.1"]), (["4.1", "4.3"], []))

    def test_three_versions(self):
        self.assertEqual(m.split_versions(VERSIONS), (["4.1", "4.3"], ["4.2"]))

    def test_four_versions(self):
        self.assertEqual(m.split_versions(FOUR_VERSIONS), (["4.1", "4.4"], ["4.2", "4.3"]))

    def test_unsorted_and_numeric_order(self):
        self.assertEqual(
            m.split_versions(["4.10", "4.2", "4.9"]), (["4.2", "4.10"], ["4.9"]))

    def test_master_is_newest(self):
        self.assertEqual(
            m.split_versions(["master", "4.1", "4.3"]), (["4.1", "master"], ["4.3"]))

    def test_empty(self):
        self.assertEqual(m.split_versions([]), ([], []))


class DecideTest(unittest.TestCase):
    def assertFull(self, d, versions=VERSIONS):
        self.assertEqual(d.mode, "full")
        self.assertEqual(d.test_spark_versions, list(versions))
        self.assertEqual(d.compile_spark_versions, [])

    def test_reduced_for_plain_source_change(self):
        d = m.decide("pull_request", VERSIONS, [], files(SRC), 1)
        self.assertEqual(d.mode, "reduced")
        self.assertEqual(d.test_spark_versions, ["4.1", "4.3"])
        self.assertEqual(d.compile_spark_versions, ["4.2"])

    def test_reduced_one_version(self):
        d = m.decide("pull_request", ["4.1"], [], files(SRC), 1)
        self.assertEqual((d.mode, d.test_spark_versions, d.compile_spark_versions),
                         ("reduced", ["4.1"], []))

    def test_reduced_two_versions(self):
        d = m.decide("pull_request", ["4.1", "4.3"], [], files(SRC), 1)
        self.assertEqual((d.test_spark_versions, d.compile_spark_versions), (["4.1", "4.3"], []))

    def test_reduced_four_versions(self):
        d = m.decide("pull_request", FOUR_VERSIONS, [], files(SRC), 1)
        self.assertEqual((d.test_spark_versions, d.compile_spark_versions),
                         (["4.1", "4.4"], ["4.2", "4.3"]))

    def test_push_is_full(self):
        self.assertFull(m.decide("push", VERSIONS, [], files(SRC), 1))

    def test_workflow_dispatch_is_full(self):
        self.assertFull(m.decide("workflow_dispatch", VERSIONS))

    def test_push_ignores_detection_inputs(self):
        self.assertFull(m.decide("push", VERSIONS, None, None, None, None))

    def test_full_ci_label(self):
        d = m.decide("pull_request", VERSIONS, ["bug", "full-ci"], files(SRC), 1)
        self.assertFull(d)
        self.assertIn("full-ci", d.reason)

    def test_unrelated_label_is_reduced(self):
        d = m.decide("pull_request", VERSIONS, ["bug", "full-ci-later"], files(SRC), 1)
        self.assertEqual(d.mode, "reduced")

    def test_full_ci_label_wins_over_detection_failure(self):
        d = m.decide("pull_request", VERSIONS, ["full-ci"], None, None, "boom")
        self.assertFull(d)
        self.assertIn("label", d.reason)

    def test_trigger_path(self):
        for path in ["build.sbt", "spark/src/main/scala-shims/spark-4.1/X.scala",
                     ".github/workflows/spark_test.yaml", "setup.py"]:
            with self.subTest(path=path):
                d = m.decide("pull_request", VERSIONS, [], files(SRC, path), 2)
                self.assertFull(d)
                self.assertEqual(d.full_matrix_files, [path])

    def test_rename_out_of_shim_dir_is_full(self):
        renamed = ["spark/src/main/scala/X.scala", "spark/src/main/scala-shims/spark-4.1/X.scala"]
        self.assertFull(m.decide("pull_request", VERSIONS, [], [renamed], 1))

    def test_detection_error_is_full(self):
        d = m.decide("pull_request", VERSIONS, [], None, None, "gh api failed")
        self.assertFull(d)
        self.assertIn("change detection failed", d.reason)

    def test_missing_file_list_is_full(self):
        self.assertFull(m.decide("pull_request", VERSIONS, [], None, 3))

    def test_empty_file_list_is_full(self):
        self.assertFull(m.decide("pull_request", VERSIONS, [], [], None))

    def test_truncated_file_list_is_full(self):
        d = m.decide("pull_request", VERSIONS, [], files(SRC), 3001)
        self.assertFull(d)
        self.assertIn("PR reports 3001", d.reason)

    def test_full_lists_are_sorted(self):
        self.assertFull(m.decide("push", ["4.3", "4.1", "4.2"]))

    def test_no_source_build_versions(self):
        for d in [m.decide("pull_request", VERSIONS, [], files(SRC), 1),
                  m.decide("push", VERSIONS)]:
            with self.subTest(mode=d.mode):
                self.assertEqual(d.source_test_spark_versions, [])


class SourceBuildTest(unittest.TestCase):
    """Source-build versions are always tested, in both modes."""

    def reduced(self, release, source):
        return m.decide("pull_request", release, [], files(SRC), 1, None, source)

    def test_reduced_one_source_build_version(self):
        d = self.reduced(VERSIONS, ["master"])
        self.assertEqual(d.mode, "reduced")
        self.assertEqual(d.test_spark_versions, ["4.1", "4.3"])
        self.assertEqual(d.compile_spark_versions, ["4.2"])
        self.assertEqual(d.source_test_spark_versions, ["master"])

    def test_reduced_two_source_build_versions(self):
        d = self.reduced(VERSIONS, ["master", "4.4"])
        self.assertEqual(d.test_spark_versions, ["4.1", "4.3"])
        self.assertEqual(d.compile_spark_versions, ["4.2"])
        self.assertEqual(d.source_test_spark_versions, ["4.4", "master"])

    def test_source_build_newer_than_newest_release_does_not_shift_split(self):
        # The newest *published* version is still tested by the regular test job.
        d = self.reduced(VERSIONS, ["4.4"])
        self.assertEqual(d.test_spark_versions, ["4.1", "4.3"])
        self.assertEqual(d.compile_spark_versions, ["4.2"])
        self.assertEqual(d.source_test_spark_versions, ["4.4"])

    def test_source_build_equal_to_newest_release_is_source_only(self):
        # A version in both lists needs source-built Spark, so it only runs as source-build,
        # and the next-newest published version becomes the newest tested release.
        d = self.reduced(FOUR_VERSIONS, ["4.4"])
        self.assertEqual(d.test_spark_versions, ["4.1", "4.3"])
        self.assertEqual(d.compile_spark_versions, ["4.2"])
        self.assertEqual(d.source_test_spark_versions, ["4.4"])

    def test_full_mode_with_source_build(self):
        for d in [m.decide("push", VERSIONS, source_build_versions=["master"]),
                  m.decide("pull_request", VERSIONS, ["full-ci"], files(SRC), 1, None, ["master"]),
                  m.decide("pull_request", VERSIONS, [], files("build.sbt"), 1, None, ["master"]),
                  m.decide("pull_request", VERSIONS, [], None, None, "boom", ["master"])]:
            with self.subTest(reason=d.reason):
                self.assertEqual(d.mode, "full")
                self.assertEqual(d.test_spark_versions, VERSIONS)
                self.assertEqual(d.compile_spark_versions, [])
                self.assertEqual(d.source_test_spark_versions, ["master"])

    def test_only_source_build_versions(self):
        d = self.reduced([], ["master"])
        self.assertEqual((d.test_spark_versions, d.compile_spark_versions,
                          d.source_test_spark_versions), ([], [], ["master"]))


class MainTest(unittest.TestCase):
    def run_main(self, *argv, file_lines=None):
        with tempfile.TemporaryDirectory() as tmp:
            out_path = os.path.join(tmp, "github_output")
            args = list(argv)
            if file_lines is not None:
                files_path = os.path.join(tmp, "files.tsv")
                Path(files_path).write_text("".join(line + "\n" for line in file_lines))
                args += ["--changed-files-file", files_path]
            old = os.environ.get("GITHUB_OUTPUT")
            os.environ["GITHUB_OUTPUT"] = out_path
            try:
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    m.main(args)
            finally:
                if old is None:
                    del os.environ["GITHUB_OUTPUT"]
                else:
                    os.environ["GITHUB_OUTPUT"] = old
            lines = Path(out_path).read_text().splitlines()
            return dict(line.split("=", 1) for line in lines)

    def test_reduced_outputs(self):
        out = self.run_main(
            "--event-name", "pull_request", "--spark-versions", json.dumps(VERSIONS),
            "--labels", "[]", "--expected-changed-files", "2",
            file_lines=[SRC, "docs/x.md\tdocs/y.md"])
        self.assertEqual(out["mode"], "reduced")
        self.assertEqual(json.loads(out["test_spark_versions"]), ["4.1", "4.3"])
        self.assertEqual(json.loads(out["compile_spark_versions"]), ["4.2"])

    # Changed-file lines in the workflow's jq format: "<filename>\t<previous_filename>".
    SHIM = "spark/src/main/scala-shims/spark-4.1/X.scala"
    PLAIN = "spark/src/main/scala/X.scala"

    def run_rename(self, line):
        return self.run_main(
            "--event-name", "pull_request", "--spark-versions", json.dumps(VERSIONS),
            "--labels", "[]", "--expected-changed-files", "1", file_lines=[line])

    def test_rename_out_of_shim_dir_is_full_end_to_end(self):
        # Only the previous path is a shim path, so this passes only if it is kept.
        out = self.run_rename(self.PLAIN + "\t" + self.SHIM)
        self.assertEqual(out["mode"], "full")
        self.assertIn(self.SHIM, out["reason"])

    def test_rename_into_shim_dir_is_full_end_to_end(self):
        out = self.run_rename(self.SHIM + "\t" + self.PLAIN)
        self.assertEqual(out["mode"], "full")
        self.assertIn(self.SHIM, out["reason"])

    def test_rename_between_plain_paths_is_reduced_end_to_end(self):
        # A rename is one changed file, so it matches the PR's changed_files count of 1.
        out = self.run_rename(self.PLAIN + "\tspark/src/main/scala/Y.scala")
        self.assertEqual(out["mode"], "reduced")

    def test_parse_changed_files_keeps_previous_path(self):
        self.assertEqual(
            m.parse_changed_files([self.PLAIN + "\t" + self.SHIM + "\n", "\n", "a.scala\n"]),
            [[self.PLAIN, self.SHIM], ["a.scala"]])

    def test_error_flag_is_full(self):
        out = self.run_main(
            "--event-name", "pull_request", "--spark-versions", json.dumps(VERSIONS),
            "--changed-files-error", "gh exited 1", "--expected-changed-files", "1",
            file_lines=[SRC])
        self.assertEqual(out["mode"], "full")
        self.assertEqual(json.loads(out["compile_spark_versions"]), [])

    def test_unreadable_file_is_full(self):
        out = self.run_main(
            "--event-name", "pull_request", "--spark-versions", json.dumps(VERSIONS),
            "--changed-files-file", "/nonexistent/files.tsv")
        self.assertEqual(out["mode"], "full")
        self.assertIn("could not read", out["reason"])

    def test_bad_labels_json_is_full(self):
        out = self.run_main(
            "--event-name", "pull_request", "--spark-versions", json.dumps(VERSIONS),
            "--labels", "not json", "--expected-changed-files", "1", file_lines=[SRC])
        self.assertEqual(out["mode"], "full")

    def test_bad_count_is_full(self):
        out = self.run_main(
            "--event-name", "pull_request", "--spark-versions", json.dumps(VERSIONS),
            "--expected-changed-files", "abc", file_lines=[SRC])
        self.assertEqual(out["mode"], "full")

    def test_push_outputs(self):
        out = self.run_main("--event-name", "push", "--spark-versions", json.dumps(VERSIONS))
        self.assertEqual(out["mode"], "full")
        self.assertEqual(json.loads(out["test_spark_versions"]), VERSIONS)

    def test_empty_versions_rejected(self):
        with self.assertRaises(ValueError):
            self.run_main("--event-name", "push", "--spark-versions", "[]")

    def test_non_list_versions_rejected(self):
        with self.assertRaises(ValueError):
            self.run_main("--event-name", "push", "--spark-versions", "{}")

    def test_source_build_outputs(self):
        for event in ["pull_request", "push"]:
            with self.subTest(event=event):
                out = self.run_main(
                    "--event-name", event, "--spark-versions", json.dumps(VERSIONS),
                    "--source-build-spark-versions", '["master"]',
                    "--expected-changed-files", "1", file_lines=[SRC])
                self.assertEqual(json.loads(out["source_test_spark_versions"]), ["master"])

    def test_source_build_default_is_empty(self):
        out = self.run_main("--event-name", "push", "--spark-versions", json.dumps(VERSIONS))
        self.assertEqual(json.loads(out["source_test_spark_versions"]), [])


if __name__ == "__main__":
    unittest.main()
