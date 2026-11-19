#!/usr/bin/env python3
"""Unit tests for auto_full_ci.py. Run: python3 -m unittest discover -s .github/scripts -p 'test_auto_full_ci.py'

The log fixtures in testdata/auto_full_ci are trimmed excerpts of real delta-io/delta
"Delta Spark" shard logs (ANSI colours, timestamps and BOM kept as GitHub returns them).
"""

import io
import json
import os
import re
import sys
import tempfile
import unittest
import zipfile
from contextlib import redirect_stderr, redirect_stdout

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import auto_full_ci as afc  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "testdata", "auto_full_ci")
REPO = "delta-io/delta"
HEAD = "a" * 40
OTHER = "b" * 40
SUITE_AMT = "org.apache.spark.sql.delta.amt.AdaptiveMetadataTableFeatureSuite"
SUITES_MERGE = {
    "org.apache.spark.sql.delta.generatedsuites.merge.MergeIntoSuiteBaseMiscSQLNameBasedSuite",
    "org.apache.spark.sql.delta.generatedsuites.merge.MergeIntoSuiteBaseMiscSQLPathBasedSuite",
    "org.apache.spark.sql.delta.generatedsuites.merge.MergeIntoSuiteBaseMiscSQLPathBasedCDCOnSuite",
}
COMPILE_SCALA = ("compile: spark/src/test/scala/org/apache/spark/sql/delta/storage/"
                 "LineClosableIteratorSuite.scala")


def fixture(name):
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return f.read()


# A shard that timed out after a test failed: a FAILED marker but no sbt summary.
TIMED_OUT_AFTER_FAILURE = (
    "2026-11-16T11:20:11.7530963Z \x1b[0m[\x1b[0m\x1b[0minfo\x1b[0m] \x1b[0m\x1b[0m\x1b[31m- "
    "some test *** FAILED ***\x1b[0m\x1b[0m\n"
    "2026-11-16T13:20:11.0000000Z ##[error]The job has exceeded the maximum execution time\n")


class FakeApi:
    """In-memory stand-in for GitHubClient. `routes` maps an API path (no paging) to a value."""

    def __init__(self, routes=None, logs=None):
        self.routes = dict(routes or {})
        self.logs = dict(logs or {})
        self.sent = []

    def _lookup(self, path):
        base = re.sub(r"[?&]per_page=100&page=(\d+)$", "", path)
        page = re.search(r"page=(\d+)$", path)
        if base not in self.routes:
            raise afc.GitHubError(f"404 {base}")
        value = self.routes[base]
        if page and int(page.group(1)) > 1:
            # Single-page fakes: later pages are empty.
            if isinstance(value, dict):
                return {k: ([] if isinstance(v, list) else v) for k, v in value.items()}
            return []
        return value

    def get_json(self, path):
        return json.loads(json.dumps(self._lookup(path)))

    def get_bytes(self, path):
        m = re.match(r"repos/[^/]+/[^/]+/actions/jobs/(\d+)/logs$", path)
        if m:
            if int(m.group(1)) not in self.logs:
                raise afc.GitHubError("404 log")
            return self.logs[int(m.group(1))].encode("utf-8")
        value = self._lookup(path)
        return value if isinstance(value, bytes) else json.dumps(value).encode()

    def send_json(self, method, path, body=None):
        self.sent.append((method, path, body))
        return {}


class RunBuilder:
    """Builds a "Delta Spark" run: jobs, logs, artifact and PR, for analyze_run."""

    def __init__(self, test=None, compile_versions=("4.2",), shards=2, attempt=1):
        # test: {version: [log-or-conclusion per shard]}; "success"/"cancelled"/... are
        # conclusions, anything else is a failure log.
        self.test = test or {}
        self.compile_versions = list(compile_versions)
        self.shards = shards
        self.attempt = attempt
        self.jobs = []
        self.logs = {}
        next_id = 1000
        self.jobs.append(self._job(next_id, "Generate Spark Versions Matrix", "success"))
        for version, outcomes in self.test.items():
            for shard, outcome in enumerate(outcomes):
                next_id += 1
                conclusion = outcome if outcome in (
                    "success", "cancelled", "skipped", "timed_out") else "failure"
                self.jobs.append(self._job(
                    next_id, f"DS: Spark {version}, Scala 2.13.16, Shard {shard}", conclusion))
                if conclusion == "failure":
                    self.logs[next_id] = outcome
        for version in self.compile_versions:
            next_id += 1
            self.jobs.append(self._job(
                next_id, f"DS: Spark {version}, Scala 2.13.16, Compile", "success"))
        self.pr = {
            "number": 42, "state": "open", "labels": [{"name": "Spark"}],
            "user": {"login": "contributor-1"},
            "head": {"sha": HEAD, "repo": {"full_name": "someone/delta"}},
            "base": {"repo": {"full_name": REPO}},
        }
        self.matrix = {
            "pr": 42, "head_sha": HEAD,
            "mode": "reduced" if self.compile_versions else "full",
            "test_spark_versions": sorted(self.test),
            "compile_spark_versions": self.compile_versions,
        }
        self.run = {
            "id": 77, "name": "Delta Spark", "event": "pull_request", "status": "completed",
            "head_sha": HEAD, "run_attempt": attempt, "pull_requests": [],
            "head_repository": {"full_name": "someone/delta"},
            "html_url": "https://github.com/delta-io/delta/actions/runs/77",
        }

    def _job(self, job_id, name, conclusion):
        return {"id": job_id, "name": name, "conclusion": conclusion, "run_attempt": self.attempt,
                "html_url": f"https://github.com/delta-io/delta/actions/runs/77/job/{job_id}"}

    def api(self):
        blob = io.BytesIO()
        with zipfile.ZipFile(blob, "w") as zf:
            zf.writestr("pr-matrix.json", json.dumps(self.matrix))
        return FakeApi({
            f"repos/{REPO}/actions/runs/77/jobs?filter=latest": {"jobs": self.jobs},
            f"repos/{REPO}/actions/runs/77/artifacts": {"artifacts": [
                {"id": 5, "name": f"pr-matrix-attempt-{self.attempt}", "size_in_bytes": 300,
                 "expired": False}]},
            f"repos/{REPO}/actions/artifacts/5/zip": blob.getvalue(),
            f"repos/{REPO}/pulls/42": self.pr,
            f"repos/{REPO}/issues/42/comments": [],
        }, self.logs)

    def analyze(self, offline=False, api=None):
        return afc.analyze_run(api or self.api(), REPO, self.run, offline=offline)


FAIL_AMT = fixture("scalatest_failed.log")
FAIL_MERGE = fixture("scalatest_aborted.log")
FAIL_COMPILE = fixture("compile_scala.log")
INFRA_UPLOAD = fixture("infra_artifact_upload.log")
OK = "success"


class ParseLogTest(unittest.TestCase):
    def test_scalatest_failed(self):
        f = afc.parse_log(FAIL_AMT)
        self.assertEqual(f.suites, {SUITE_AMT})
        self.assertTrue(f.real_failure)
        self.assertTrue(f.complete)
        self.assertEqual(f.keys(), {SUITE_AMT})

    def test_scalatest_aborted(self):
        f = afc.parse_log(FAIL_MERGE)
        self.assertEqual(f.suites, SUITES_MERGE)
        self.assertTrue(f.complete)

    def test_compile_scala(self):
        f = afc.parse_log(FAIL_COMPILE)
        self.assertTrue(f.compile_error)
        self.assertEqual(f.keys(), {COMPILE_SCALA})
        self.assertTrue(f.complete)

    def test_compile_java(self):
        f = afc.parse_log(fixture("compile_java.log"))
        self.assertEqual(f.compile_tasks, {"sparkV2"})
        self.assertEqual(f.keys(), {
            "compile: spark/v2/src/test/java/org/apache/spark/sql/delta/v2/read/"
            "DeltaV2MicroBatchStreamTest.java"})

    def test_lint_is_not_a_real_failure(self):
        f = afc.parse_log(fixture("lint_scalafmt.log"))
        self.assertFalse(f.real_failure)
        self.assertTrue(f.lint)

    def test_infra_failures_are_not_real_failures(self):
        f = afc.parse_log(INFRA_UPLOAD)
        self.assertFalse(f.real_failure)
        self.assertIn("artifact upload", f.infra_hints)
        f = afc.parse_log(fixture("infra_spark_cache_missing.log"))
        self.assertFalse(f.real_failure)
        self.assertIn("Spark cache missing", f.infra_hints)

    def test_failed_marker_without_summary_is_incomplete(self):
        f = afc.parse_log(TIMED_OUT_AFTER_FAILURE)
        self.assertTrue(f.real_failure)
        self.assertFalse(f.complete)

    def test_untrusted_summary_lines_are_ignored(self):
        f = afc.parse_log(
            "[error] Failed tests:\n"
            "[error] \t@everyone [x](https://evil.example)\n"
            "[error] \torg.example.RealSuite\n")
        # The block ends at the first line that isn't a class name.
        self.assertEqual(f.suites, set())
        self.assertTrue(f.test_marker)
        f = afc.parse_log("[error] Failed tests:\n[error] \torg.example.RealSuite\n")
        self.assertEqual(f.suites, {"org.example.RealSuite"})


class DivergenceTest(unittest.TestCase):
    def test_one_pass_one_fail_labels(self):
        a = RunBuilder({"4.1": [OK, FAIL_AMT], "4.3": [OK, OK]}).analyze()
        self.assertTrue(a.label, a.reason)
        self.assertEqual(a.results["4.1"].keys, {SUITE_AMT})
        self.assertEqual(a.results["4.3"].status, afc.PASS)
        self.assertIn("4.3 passed but Spark 4.1 failed", a.reason)

    def test_both_fail_same_suites_no_label(self):
        # Shard indices differ by version; only suite names are compared.
        a = RunBuilder({"4.1": [FAIL_AMT, OK], "4.3": [OK, FAIL_AMT]}).analyze()
        self.assertFalse(a.label)
        self.assertIn("same suites", a.reason)

    def test_both_fail_different_suites_labels(self):
        a = RunBuilder({"4.1": [FAIL_AMT, FAIL_MERGE], "4.3": [OK, FAIL_MERGE]}).analyze()
        self.assertTrue(a.label, a.reason)
        self.assertIn(f"`{SUITE_AMT}`", a.body)
        self.assertIn("Failing on all tested versions", a.body)

    def test_both_pass_no_label(self):
        a = RunBuilder({"4.1": [OK, OK], "4.3": [OK, OK]}).analyze()
        self.assertFalse(a.label)
        self.assertEqual(a.reason, "all tested Spark versions passed")

    def test_infra_only_failure_is_inconclusive(self):
        a = RunBuilder({"4.1": [OK, INFRA_UPLOAD], "4.3": [OK, OK]}).analyze()
        self.assertFalse(a.label)
        self.assertEqual(a.results["4.1"].status, afc.INCONCLUSIVE)
        self.assertIn("artifact upload", a.results["4.1"].reason)
        a = RunBuilder({"4.1": [OK, INFRA_UPLOAD], "4.3": [FAIL_AMT, OK]}).analyze()
        self.assertFalse(a.label)
        self.assertIn("inconclusive", a.reason)

    def test_lint_failure_is_inconclusive(self):
        a = RunBuilder({"4.1": [fixture("lint_scalafmt.log"), OK], "4.3": [OK, OK]}).analyze()
        self.assertFalse(a.label)
        self.assertIn("lint", a.results["4.1"].reason)

    def test_cancelled_timed_out_and_missing_shards_are_inconclusive(self):
        for outcome in ("cancelled", "timed_out", "skipped"):
            a = RunBuilder({"4.1": [OK, outcome], "4.3": [OK, OK]}).analyze()
            self.assertFalse(a.label, outcome)
            self.assertEqual(a.results["4.1"].status, afc.INCONCLUSIVE)
        a = RunBuilder({"4.1": [OK], "4.3": [OK, OK]}).analyze()
        self.assertFalse(a.label)
        self.assertIn("missing shards 1", a.results["4.1"].reason)

    def test_unavailable_log_is_inconclusive(self):
        b = RunBuilder({"4.1": [OK, FAIL_AMT], "4.3": [OK, OK]})
        api = b.api()
        api.logs.clear()
        a = b.analyze(api=api)
        self.assertFalse(a.label)
        self.assertIn("log unavailable", a.results["4.1"].reason)

    def test_compile_error_on_one_version_labels(self):
        a = RunBuilder({"4.1": [FAIL_COMPILE, FAIL_COMPILE], "4.3": [OK, OK]}).analyze()
        self.assertTrue(a.label, a.reason)
        self.assertEqual(a.results["4.1"].keys, {COMPILE_SCALA})
        self.assertIn(f"`{COMPILE_SCALA}`", a.body)

    def test_compile_error_on_both_versions_no_label(self):
        a = RunBuilder({"4.1": [FAIL_COMPILE, FAIL_COMPILE],
                        "4.3": [FAIL_COMPILE, FAIL_COMPILE]}).analyze()
        self.assertFalse(a.label)

    def test_failure_plus_inconclusive_shard(self):
        # Pass vs real failure is trusted even if the failing version has an infra shard.
        a = RunBuilder({"4.1": [FAIL_AMT, INFRA_UPLOAD], "4.3": [OK, OK]}).analyze()
        self.assertTrue(a.label, a.reason)
        # Fail vs fail with a suite list that may be incomplete is not.
        a = RunBuilder({"4.1": [FAIL_AMT, INFRA_UPLOAD], "4.3": [FAIL_MERGE, OK]}).analyze()
        self.assertFalse(a.label)
        self.assertIn("incomplete", a.reason)

    def test_timed_out_after_failure(self):
        a = RunBuilder({"4.1": [TIMED_OUT_AFTER_FAILURE, OK], "4.3": [OK, OK]}).analyze()
        self.assertTrue(a.label, a.reason)
        a = RunBuilder({"4.1": [TIMED_OUT_AFTER_FAILURE, OK], "4.3": [FAIL_AMT, OK]}).analyze()
        self.assertFalse(a.label)

    def test_three_versions(self):
        a = RunBuilder({"4.1": [FAIL_AMT, OK], "4.2": [OK, OK], "4.3": [FAIL_AMT, OK]},
                       compile_versions=["4.15"]).analyze()
        self.assertTrue(a.label, a.reason)
        self.assertIn("Spark 4.2 passed but Spark 4.1, 4.3 failed", a.reason)
        self.assertIn("Failing on some, but not all, tested versions", a.body)
        a = RunBuilder({"4.1": [FAIL_AMT, OK], "4.2": [FAIL_AMT, OK], "4.3": [OK, FAIL_AMT]},
                       compile_versions=["4.15"]).analyze()
        self.assertFalse(a.label)
        a = RunBuilder({"4.1": [FAIL_AMT, OK], "4.2": [FAIL_AMT, OK],
                        "4.3": [FAIL_AMT, FAIL_MERGE]}, compile_versions=["4.15"]).analyze()
        self.assertTrue(a.label, a.reason)
        self.assertIn("**Failing only on Spark `4.3`:**", a.body)
        self.assertIn("Failing on all tested versions", a.body)
        a = RunBuilder({"4.1": [FAIL_AMT, OK], "4.2": ["cancelled", OK], "4.3": [OK, OK]},
                       compile_versions=["4.15"]).analyze()
        self.assertFalse(a.label)

    def test_single_tested_version(self):
        a = RunBuilder({"4.1": [FAIL_AMT]}).analyze()
        self.assertFalse(a.label)


class ContextTest(unittest.TestCase):
    """Run, artifact and PR validation in analyze_run."""

    DIVERGENT = {"4.1": [OK, FAIL_AMT], "4.3": [OK, OK]}

    def test_full_mode_run_is_noop(self):
        a = RunBuilder(self.DIVERGENT, compile_versions=()).analyze()
        self.assertFalse(a.label)
        self.assertIn("full-matrix", a.reason)

    def test_skipped_compile_job_means_full_mode(self):
        b = RunBuilder(self.DIVERGENT, compile_versions=())
        b.jobs.append({"id": 9, "name": "DS: Spark ${{ matrix.spark_version }}, Scala "
                       "${{ matrix.scala }}, Compile", "conclusion": "skipped"})
        self.assertFalse(b.analyze().label)

    def test_artifact_claiming_full_mode_doesnt_matter(self):
        # Mode comes from the jobs; a disagreeing artifact makes the run untrusted.
        b = RunBuilder(self.DIVERGENT)
        b.matrix["mode"] = "full"
        a = b.analyze()
        self.assertFalse(a.label)
        self.assertIn("mode", a.reason)

    def test_non_pr_and_other_workflow_runs_are_noop(self):
        b = RunBuilder(self.DIVERGENT)
        b.run["event"] = "push"
        self.assertFalse(b.analyze().label)
        b = RunBuilder(self.DIVERGENT)
        b.run["name"] = "Delta Kernel"
        self.assertFalse(b.analyze().label)

    def test_already_labelled_is_noop(self):
        b = RunBuilder(self.DIVERGENT)
        b.pr["labels"].append({"name": "full-ci"})
        a = b.analyze()
        self.assertFalse(a.label)
        self.assertIn("already has", a.reason)

    def test_forged_pr_number(self):
        # The artifact names another PR, whose head isn't this run's commit.
        b = RunBuilder(self.DIVERGENT)
        b.pr["head"]["sha"] = OTHER
        a = b.analyze()
        self.assertFalse(a.label)
        self.assertIn("forged", a.reason)

    def test_artifact_sha_mismatch(self):
        b = RunBuilder(self.DIVERGENT)
        b.matrix["head_sha"] = OTHER
        self.assertIn("head_sha", b.analyze().reason)

    def test_head_repo_mismatch(self):
        b = RunBuilder(self.DIVERGENT)
        b.pr["head"]["repo"]["full_name"] = "attacker/delta"
        self.assertIn("head repository", b.analyze().reason)

    def test_pull_requests_list_must_contain_artifact_pr(self):
        b = RunBuilder(self.DIVERGENT)
        b.run["pull_requests"] = [{"number": 7}]
        self.assertIn("isn't one of", b.analyze().reason)
        b.run["pull_requests"] = [{"number": 42}]
        self.assertTrue(b.analyze().label)

    def test_artifact_versions_must_match_jobs(self):
        b = RunBuilder(self.DIVERGENT)
        b.matrix["test_spark_versions"] = ["4.1", "4.2"]
        self.assertIn("versions", b.analyze().reason)

    def test_closed_pr_is_noop(self):
        b = RunBuilder(self.DIVERGENT)
        b.pr["state"] = "closed"
        self.assertFalse(b.analyze().label)

    def test_missing_or_bad_artifact(self):
        b = RunBuilder(self.DIVERGENT)
        api = b.api()
        api.routes[f"repos/{REPO}/actions/runs/77/artifacts"] = {"artifacts": []}
        self.assertIn("artifact", b.analyze(api=api).reason)
        api = b.api()
        api.routes[f"repos/{REPO}/actions/artifacts/5/zip"] = b"not a zip"
        self.assertIn("artifact", b.analyze(api=api).reason)

    def test_rerun_attempt_uses_latest_jobs_and_artifact(self):
        b = RunBuilder(self.DIVERGENT, attempt=2)
        # Attempt 1 of the 4.3 shard 1 failed; attempt 2 passed. Only attempt 2 counts.
        old = dict(b.jobs[-2], id=999, run_attempt=1, conclusion="failure")
        b.logs[999] = FAIL_AMT
        b.jobs.insert(0, old)
        api = b.api()
        artifacts = api.routes[f"repos/{REPO}/actions/runs/77/artifacts"]["artifacts"]
        # An attempt-1 artifact with another SHA, and a (bogus) attempt-3 one, are ignored.
        artifacts += [{"id": 6, "name": "pr-matrix-attempt-1", "size_in_bytes": 1},
                      {"id": 8, "name": "pr-matrix-attempt-3", "size_in_bytes": 1}]
        a = b.analyze(api=api)
        self.assertTrue(a.label, a.reason)
        self.assertEqual(a.results["4.3"].status, afc.PASS)

    def test_offline_mode_ignores_artifact_and_pr(self):
        b = RunBuilder(self.DIVERGENT, compile_versions=())
        api = b.api()
        del api.routes[f"repos/{REPO}/pulls/42"]
        a = b.analyze(offline=True, api=api)
        self.assertTrue(a.label)
        self.assertIsNone(a.pr)


class PrMatrixTest(unittest.TestCase):
    def good(self, **kw):
        d = {"pr": 1, "head_sha": HEAD, "mode": "reduced", "test_spark_versions": ["4.3", "4.1"],
             "compile_spark_versions": ["4.2"]}
        d.update(kw)
        return json.dumps(d).encode()

    def test_valid(self):
        pm = afc.parse_pr_matrix(self.good())
        self.assertEqual(pm.test_spark_versions, ["4.1", "4.3"])

    def test_invalid(self):
        for bad in ({"pr": True}, {"pr": "1"}, {"pr": 0}, {"head_sha": "abc"},
                    {"mode": "x"}, {"test_spark_versions": ["4.1; rm -rf /"]},
                    {"compile_spark_versions": "4.2"}):
            with self.assertRaises(ValueError, msg=bad):
                afc.parse_pr_matrix(self.good(**bad))
        with self.assertRaises(ValueError):
            afc.parse_pr_matrix(b"[]")


class CommentTest(unittest.TestCase):
    def test_comment_content(self):
        a = RunBuilder({"4.1": [OK, FAIL_AMT], "4.3": [OK, OK]}).analyze()
        body = a.body
        self.assertTrue(body.startswith(afc.MARKER_FORMAT.format(sha=HEAD)))
        self.assertIn("@contributor-1,", body)
        self.assertIn("**Failing only on Spark `4.1`:**", body)
        self.assertIn("Passed on: Spark `4.3`.", body)
        self.assertIn("https://github.com/delta-io/delta/actions/runs/77/job/", body)
        self.assertIn("sticky", body)
        self.assertIn("until a maintainer removes it", body)

    def test_escaping(self):
        self.assertEqual(afc.md_text("@evil <img src=x>"), r"\@evil \<img src\=x\>")
        self.assertEqual(afc.md_code("a`b"), r"a\`b")
        self.assertEqual(afc.md_code("org.x.Suite"), "`org.x.Suite`")
        self.assertIsNone(afc.md_url("javascript:alert(1)"))
        self.assertIsNone(afc.md_url("https://evil.example/x"))
        results = {
            "4.1": afc.VersionResult("4.1", afc.FAIL, {"x"}, True, [afc.ShardResult(
                "DS: Spark 4.1 [evil](https://e.example) @team", "https://e.example", afc.FAIL)]),
            "4.3": afc.VersionResult("4.3", afc.PASS),
        }
        body = afc.render_comment("bad login!", HEAD, results, afc.Divergence(True, "r"), None)
        self.assertNotIn("](https://e.example)", body)
        self.assertNotIn(" @team", body)
        self.assertNotIn("@bad", body)


class ApplyTest(unittest.TestCase):
    def setUp(self):
        self.body = afc.MARKER_FORMAT.format(sha=HEAD) + "\nhello\n"
        self.pr = {"number": 42, "state": "open", "labels": [],
                   "head": {"sha": HEAD}, "base": {"repo": {"full_name": REPO}}}

    def api(self, comments=()):
        return FakeApi({f"repos/{REPO}/pulls/42": self.pr,
                        f"repos/{REPO}/issues/42/comments": list(comments)})

    def test_labels_and_comments(self):
        api = self.api()
        actions = afc.apply_label(api, REPO, 42, HEAD, self.body)
        self.assertEqual(api.sent, [
            ("POST", f"repos/{REPO}/issues/42/labels", {"labels": ["full-ci"]}),
            ("POST", f"repos/{REPO}/issues/42/comments", {"body": self.body}),
        ])
        self.assertEqual(actions, ["added full-ci", "posted comment"])

    def test_rerun_fallback(self):
        api, rerun = self.api(), FakeApi()
        afc.apply_label(api, REPO, 42, HEAD, self.body, rerun_api=rerun, run_id=77)
        self.assertEqual(rerun.sent, [("POST", f"repos/{REPO}/actions/runs/77/rerun", None)])

    def test_dedup_by_bot_marker(self):
        api = self.api([{"body": self.body, "user": {"type": "Bot"}}])
        actions = afc.apply_label(api, REPO, 42, HEAD, self.body, rerun_api=FakeApi(), run_id=77)
        self.assertEqual(api.sent, [])
        self.assertIn("already labelled", actions[0])

    def test_user_marker_does_not_suppress(self):
        api = self.api([{"body": self.body, "user": {"type": "User"}}])
        afc.apply_label(api, REPO, 42, HEAD, self.body)
        self.assertEqual(len(api.sent), 2)

    def test_marker_for_older_head_does_not_suppress(self):
        old = afc.MARKER_FORMAT.format(sha=OTHER)
        api = self.api([{"body": old, "user": {"type": "Bot"}}])
        afc.apply_label(api, REPO, 42, HEAD, self.body)
        self.assertEqual(len(api.sent), 2)

    def test_head_moved_or_labelled(self):
        self.pr["head"]["sha"] = OTHER
        api = self.api()
        afc.apply_label(api, REPO, 42, HEAD, self.body)
        self.assertEqual(api.sent, [])
        self.pr["head"]["sha"] = HEAD
        self.pr["labels"] = [{"name": "full-ci"}]
        afc.apply_label(api, REPO, 42, HEAD, self.body)
        self.assertEqual(api.sent, [])


class CliTest(unittest.TestCase):
    def test_analyze_from_event_file(self):
        b = RunBuilder({"4.1": [OK, FAIL_AMT], "4.3": [OK, OK]})
        with tempfile.TemporaryDirectory() as tmp:
            event = os.path.join(tmp, "event.json")
            with open(event, "w") as f:
                json.dump({"workflow_run": b.run}, f)
            out = os.path.join(tmp, "out")
            body = os.path.join(tmp, "body.md")
            args = afc.argparse.Namespace(repo=REPO, event_path=event, run_id=None,
                                          offline=False, body_file=body)
            old = os.environ.get("GITHUB_OUTPUT")
            os.environ["GITHUB_OUTPUT"] = out
            try:
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    afc.cmd_analyze(args, api=b.api())
            finally:
                if old is None:
                    del os.environ["GITHUB_OUTPUT"]
                else:
                    os.environ["GITHUB_OUTPUT"] = old
            with open(out) as f:
                outputs = dict(line.rstrip("\n").split("=", 1) for line in f)
            body_b64 = outputs.pop("body_b64")
            self.assertEqual(outputs, {"should_label": "true", "would_label": "true",
                                       "pr": "42", "head_sha": HEAD})
            with open(body) as f:
                text = f.read()
            self.assertIn("@contributor-1", text)
            self.assertEqual(afc.base64.b64decode(body_b64).decode(), text)

            # The body travels to the apply job as body_b64.
            api = FakeApi({f"repos/{REPO}/pulls/42": b.pr,
                           f"repos/{REPO}/issues/42/comments": []})
            rerun = FakeApi()
            args = afc.argparse.Namespace(repo=REPO, pr=42, head_sha=HEAD, body_file=None,
                                          body_b64=body_b64, run_id="77", rerun=True)
            with redirect_stdout(io.StringIO()):
                afc.cmd_apply(args, api=api, rerun_api=rerun)
            self.assertEqual(api.sent[1], ("POST", f"repos/{REPO}/issues/42/comments",
                                           {"body": text}))
            self.assertEqual(len(rerun.sent), 1)

    def test_offline_never_sets_should_label(self):
        b = RunBuilder({"4.1": [OK, FAIL_AMT], "4.3": [OK, OK]}, compile_versions=())
        api = b.api()
        api.routes[f"repos/{REPO}/actions/runs/77"] = b.run
        args = afc.argparse.Namespace(repo=REPO, event_path=None, run_id="77", offline=True,
                                      body_file=None)
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(io.StringIO()):
            afc.cmd_analyze(args, api=api)
        self.assertIn("should_label=false", out.getvalue())
        self.assertIn("would_label=true", out.getvalue())
        self.assertIn("body_b64=\n", out.getvalue())


if __name__ == "__main__":
    unittest.main()
