#!/usr/bin/env python3
"""
Add the `full-ci` label to a PR when, on a reduced "Delta Spark" run, the tested Spark
versions disagree.

The "Delta Spark" workflow (.github/workflows/spark_test.yaml) runs a reduced matrix on
most PRs: sharded tests on the oldest and newest published Spark versions, and a compile
on the versions in between (see project/scripts/spark_ci_matrix.py). The "Auto full-ci"
workflow (.github/workflows/auto_full_ci.yaml) runs this script when such a run
completes. If the tested versions diverge, the PR gets the sticky `full-ci` label, which
makes every later run of the PR use the full matrix, and one comment for the PR author.

Divergence. Each tested Spark version is classified from its shard jobs, against the
expected shard set read from the trusted (default-branch) spark_test.yaml:
  * pass: every expected shard ran and concluded `success`;
  * fail(keys): at least one shard failed with a real failure, whatever the other shards
    did. Keys are the failing test suite names (sbt "Failed tests:" / "Error during
    tests:" blocks and ScalaTest "<Suite> *** ABORTED ***" lines) plus "compile: <file>"
    for compile errors. The key list is complete only if every other expected shard
    passed and every failing shard printed sbt's summary;
  * inconclusive: no real failure, but some expected shard is missing, or was cancelled,
    timed out or skipped, or failed without a test or compile marker (lost runner, OOM,
    dependency download, artifact upload, lint).
The versions diverge, and the PR is labelled, if one version passes and another fails,
or if all fail with different key sets. Keys are suite names, never shard indices,
because shard assignment can differ between Spark versions. No label is added when any
tested version is inconclusive, or when all fail and any of them has an incomplete key
list.

Trust. Everything that comes from the run (the pr-matrix artifact, log text, suite and
branch names) is untrusted: a fork PR can change spark_test.yaml. The PR number from
the artifact is accepted only if that PR's head SHA and repository match the
workflow_run event, and the matrix mode and versions are recomputed from the run's job
names. Log-derived names are only accepted if they match strict patterns, and every
string is escaped before it goes into Markdown.

Subcommands:
  analyze  Read-only. Decide whether to label. In a workflow it reads the workflow_run
           event from $GITHUB_EVENT_PATH and writes outputs to $GITHUB_OUTPUT. With
           --run-id and --offline it analyses any historical run without the artifact
           or PR checks (for manual verification).
  apply    Make sure the PR has the label, one comment for this head SHA, and a full run.
           Each step is idempotent, so a retry after a partial failure finishes the rest.
           A hidden marker in the comment records that the label was added for this
           SHA: the comment is never posted twice, and a label a maintainer removed
           isn't added again for the same commit. A label added with an App token
           starts the full run; otherwise (GITHUB_TOKEN, whose events don't start
           workflows) the evaluated run is re-run, unless it already has a newer attempt.

GitHub API calls use the token in $GH_TOKEN (stdlib only; no third-party packages).
To check a historical run by hand (read-only):
    GH_TOKEN=$(gh auth token) python3 .github/scripts/auto_full_ci.py analyze \\
      --repo delta-io/delta --run-id 42934767105 --offline
"""

import argparse
import base64
import io
import json
import os
import re
import sys
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Set

FULL_CI_LABEL = "full-ci"
WORKFLOW_NAME = "Delta Spark"
ARTIFACT_PREFIX = "pr-matrix-attempt-"
ARTIFACT_FILE = "pr-matrix.json"
MARKER_FORMAT = "<!-- auto-full-ci head={sha} -->"
MARKER_RE = re.compile(r"<!-- auto-full-ci head=([0-9a-f]{40}) -->")

# Job names from spark_test.yaml.
VERSION_PATTERN = r"[0-9A-Za-z][0-9A-Za-z._-]{0,31}"
TEST_JOB_RE = re.compile(
    r"^DS: Spark (?P<version>" + VERSION_PATTERN + r"), Scala (?P<scala>[0-9.]+), "
    r"Shard (?P<shard>\d{1,3})$")
COMPILE_JOB_RE = re.compile(
    r"^DS: Spark (?P<version>" + VERSION_PATTERN + r"), Scala (?P<scala>[0-9.]+), Compile$")
VERSION_RE = re.compile("^" + VERSION_PATTERN + "$")
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
LOGIN_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})(?:\[bot\])?$")
REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")

# Log parsing.
_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
_TIMESTAMP_RE = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z ?")
_IDENT = r"[A-Za-z_$][A-Za-z0-9_$]*"
_CLASS = _IDENT + r"(?:\." + _IDENT + r")*"
_CLASS_RE = re.compile("^" + _CLASS + "$")
_SUMMARY_START_RE = re.compile(r"^\[error\]\s*(Failed tests:|Error during tests:)\s*$")
_SUMMARY_ITEM_RE = re.compile(r"^\[error\]\s+(" + _CLASS + r")\s*$")
_ABORTED_SUITE_RE = re.compile(r"^\[info\]\s+(" + _CLASS + r") \*\*\* ABORTED \*\*\*")
_TEST_MARKER_RE = re.compile(
    r"\*\*\* (?:FAILED|ABORTED|RUN ABORTED) \*\*\*|\*\*\* \d+ (?:TESTS?|SUITES?) "
    r"(?:FAILED|ABORTED) \*\*\*|sbt\.TestsFailedException")
_COMPILE_TASK_RE = re.compile(
    r"^\[error\] \(([A-Za-z0-9_-]+) / (?:[A-Za-z]+ / )?compileIncremental\) "
    r"(?:Compilation failed|javac returned non-zero exit code)")
_COMPILE_FILE_RE = re.compile(
    r"^\[error\] (?:/home/runner/work/[^/\s]+/[^/\s]+/)?(?P<path>[A-Za-z0-9_./-]+\.(?:scala|java))"
    r":\d+(?::\d+)?: ")
_LINT_TASK_RE = re.compile(
    r"^\[error\] \([A-Za-z0-9_-]+ / (?:[A-Za-z]+ / )?"
    r"(?:scalafmtCheck|scalafmtSbtCheck|javafmtCheck|scalastyle|headerCheck)\)")
_INFRA_HINTS = (
    ("runner lost", re.compile(r"lost communication with the server|runner has received a "
                               r"shutdown signal|The operation was canceled")),
    ("out of memory", re.compile(r"java\.lang\.OutOfMemoryError|Killed\s*$|exit code 137")),
    ("artifact upload", re.compile(r"Failed to CreateArtifact|Failed to FinalizeArtifact")),
    ("dependency download", re.compile(r"(?i)(download failed|could not resolve|"
                                       r"unresolved dependency|error downloading)")),
    ("Spark cache missing", re.compile(r"Prepared Spark Maven cache was not found")),
)

MAX_LOG_BYTES = 64 * 1024 * 1024
MAX_ARTIFACT_BYTES = 1024 * 1024
MAX_LISTED = 25


# --------------------------------------------------------------------------------------
# Log parsing and classification
# --------------------------------------------------------------------------------------

@dataclass
class LogFindings:
    suites: Set[str] = field(default_factory=set)
    compile_files: Set[str] = field(default_factory=set)
    compile_tasks: Set[str] = field(default_factory=set)
    test_marker: bool = False
    summary_block: bool = False
    lint: bool = False
    infra_hints: List[str] = field(default_factory=list)

    @property
    def compile_error(self) -> bool:
        return bool(self.compile_tasks)

    @property
    def real_failure(self) -> bool:
        return bool(self.suites) or self.test_marker or self.compile_error

    def keys(self) -> Set[str]:
        keys = set(self.suites)
        if self.compile_error:
            if self.compile_files:
                keys.update("compile: " + p for p in self.compile_files)
            else:
                keys.update("compile: " + t for t in self.compile_tasks)
        return keys

    @property
    def complete(self) -> bool:
        """Whether keys() lists every failure in the log."""
        if self.test_marker and not self.summary_block:
            # E.g. a shard that timed out after a test failed: sbt never printed the
            # list of failing suites, and the *** FAILED *** lines of parallel suites are
            # interleaved, so they can't be attributed to a suite.
            return False
        return bool(self.keys())


def _clean_line(raw: str) -> str:
    line = _ANSI_RE.sub("", raw).lstrip("\ufeff")
    return _TIMESTAMP_RE.sub("", line).rstrip("\r\n")


def _repo_relative(path: str) -> str:
    # Keep the path from the first well-known top-level directory, without "..".
    parts = [p for p in path.split("/") if p not in ("", ".", "..")]
    return "/".join(parts)


def parse_log(text: str) -> LogFindings:
    """Extract test, compile and lint failure markers from a job log (untrusted text)."""
    f = LogFindings()
    in_summary = False
    for raw in text.splitlines():
        line = _clean_line(raw)
        if in_summary:
            m = _SUMMARY_ITEM_RE.match(line)
            if m:
                f.suites.add(m.group(1))
                continue
            in_summary = False
        if _SUMMARY_START_RE.match(line):
            in_summary = True
            f.summary_block = True
            f.test_marker = True
            continue
        m = _ABORTED_SUITE_RE.match(line)
        if m:
            f.suites.add(m.group(1))
            f.test_marker = True
            continue
        if _TEST_MARKER_RE.search(line):
            f.test_marker = True
        m = _COMPILE_TASK_RE.match(line)
        if m:
            f.compile_tasks.add(m.group(1))
            continue
        m = _COMPILE_FILE_RE.match(line)
        if m:
            f.compile_files.add(_repo_relative(m.group("path")))
            continue
        if _LINT_TASK_RE.match(line):
            f.lint = True
        for name, pattern in _INFRA_HINTS:
            if name not in f.infra_hints and pattern.search(line):
                f.infra_hints.append(name)
    if not f.compile_error:
        # Without a failed compile task, "[error] File.scala:1:2: ..." lines are lint
        # (e.g. scalastyle) output, not compile errors.
        f.compile_files.clear()
    return f


PASS, FAIL, INCONCLUSIVE = "pass", "fail", "inconclusive"


@dataclass
class ShardResult:
    name: str
    url: str
    status: str
    keys: Set[str] = field(default_factory=set)
    complete: bool = True
    reason: str = ""


@dataclass
class VersionResult:
    version: str
    status: str
    keys: Set[str] = field(default_factory=set)
    complete: bool = True
    shards: List[ShardResult] = field(default_factory=list)
    reason: str = ""

    def failing_jobs(self) -> List[ShardResult]:
        return [s for s in self.shards if s.status == FAIL]


def classify_shard(job: dict, log_fetcher: Callable[[int], Optional[str]]) -> ShardResult:
    name = str(job.get("name", ""))
    url = str(job.get("html_url", ""))
    conclusion = job.get("conclusion")
    if conclusion == "success":
        return ShardResult(name, url, PASS)
    if conclusion != "failure":
        return ShardResult(name, url, INCONCLUSIVE, reason=f"job {conclusion or 'not finished'}")
    text = log_fetcher(int(job["id"]))
    if text is None:
        return ShardResult(name, url, INCONCLUSIVE, reason="job log unavailable")
    findings = parse_log(text)
    if findings.real_failure:
        return ShardResult(name, url, FAIL, findings.keys(), findings.complete)
    if findings.lint:
        reason = "failed in a lint check before tests ran"
    elif findings.infra_hints:
        reason = "failed without test or compile errors (" + ", ".join(findings.infra_hints) + ")"
    else:
        reason = "failed without test or compile errors"
    return ShardResult(name, url, INCONCLUSIVE, reason=reason)


def latest_jobs(jobs: Iterable[dict]) -> List[dict]:
    """Keep only the latest attempt of each job name (the jobs API may return several)."""
    by_name: Dict[str, dict] = {}
    for job in jobs:
        name = str(job.get("name", ""))
        prev = by_name.get(name)
        if prev is None or (job.get("run_attempt") or 0, job.get("id") or 0) >= (
                prev.get("run_attempt") or 0, prev.get("id") or 0):
            by_name[name] = job
    return list(by_name.values())


def _version_sort_key(version: str):
    if version == "master":
        return (1, ())
    return (0, tuple(int(p) if p.isdigit() else -1 for p in version.split(".")))


def sort_versions(versions: Iterable[str]) -> List[str]:
    return sorted(set(versions), key=_version_sort_key)


@dataclass
class RunMatrix:
    mode: str  # "reduced" or "full"
    test_versions: List[str]
    compile_versions: List[str]
    test_jobs: Dict[str, List[dict]]


def matrix_from_jobs(jobs: Sequence[dict]) -> RunMatrix:
    """Recompute the matrix mode and versions from the run's own job names.

    A reduced run is one with compile jobs: the full matrix tests every version and
    compiles none. Skipped matrix jobs keep an unexpanded name, so they don't match.
    """
    test_jobs: Dict[str, List[dict]] = {}
    compile_versions = set()
    for job in jobs:
        name = str(job.get("name", ""))
        m = TEST_JOB_RE.match(name)
        if m:
            test_jobs.setdefault(m.group("version"), []).append(job)
            continue
        m = COMPILE_JOB_RE.match(name)
        if m and job.get("conclusion") != "skipped":
            compile_versions.add(m.group("version"))
    mode = "reduced" if compile_versions else "full"
    return RunMatrix(mode, sort_versions(test_jobs), sort_versions(compile_versions), test_jobs)


def expected_shards_from_workflow(text: str) -> List[str]:
    """Read the shard list of the `test` job's matrix from spark_test.yaml.

    The privileged workflow reads this from the default-branch checkout, never from the
    PR. The job's `num-shards` input must agree with the list, which must be 0..N-1.
    """
    m = re.search(r"^  test:\n(.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)", text, re.M | re.S)
    if not m:
        raise ValueError("spark_test.yaml has no `test` job")
    job = m.group(1)
    lists = re.findall(r"^\s+shard: \[([0-9,\s]*)\]\s*$", job, re.M)
    counts = re.findall(r"^\s+num-shards: '?(\d+)'?\s*$", job, re.M)
    if len(lists) != 1 or len(counts) != 1:
        raise ValueError("can't find exactly one shard list and num-shards in the test job")
    shards = [int(x) for x in lists[0].replace(",", " ").split()]
    n = int(counts[0])
    if n <= 0 or shards != list(range(n)):
        raise ValueError(f"shard list {shards} doesn't match num-shards {n}")
    return [str(i) for i in shards]


def classify_versions(
    test_jobs: Dict[str, List[dict]],
    log_fetcher: Callable[[int], Optional[str]],
    expected_shards: Sequence[str],
) -> Dict[str, VersionResult]:
    """Classify each tested version against the trusted expected shard set.

    A version passes only if every expected shard ran and succeeded. Any real failure
    makes it fail; its suite list is complete only if every other expected shard passed.
    Otherwise (missing, unexpected or inconclusive shards, no real failure) it is
    inconclusive.
    """
    expected = set(expected_shards)
    results = {}
    for version in sort_versions(test_jobs):
        shards = [classify_shard(j, log_fetcher)
                  for j in sorted(test_jobs[version], key=lambda j: j["name"])]
        present = {TEST_JOB_RE.match(s.name).group("shard") for s in shards}
        missing = sorted(expected - present, key=int)
        unexpected = sorted(present - expected, key=int)
        fails = [s for s in shards if s.status == FAIL]
        inconclusive = [s for s in shards if s.status == INCONCLUSIVE]
        reasons = [f"{s.name}: {s.reason}" for s in inconclusive]
        if missing:
            reasons.append("missing shards " + ", ".join(missing))
        if unexpected:
            reasons.append("unexpected shards " + ", ".join(unexpected))
        if fails:
            keys = set().union(*(s.keys for s in fails))
            complete = (not inconclusive and not missing and not unexpected and
                        all(s.complete for s in fails))
            results[version] = VersionResult(version, FAIL, keys, complete, shards,
                                             "; ".join(reasons))
        elif reasons:
            results[version] = VersionResult(
                version, INCONCLUSIVE, set(), False, shards, "; ".join(reasons))
        else:
            results[version] = VersionResult(version, PASS, set(), True, shards)
    return results


@dataclass
class Divergence:
    label: bool
    reason: str


def decide_divergence(results: Dict[str, VersionResult]) -> Divergence:
    """Decide whether the tested versions diverge (works for any N >= 2)."""
    if len(results) < 2:
        return Divergence(False, f"only {len(results)} tested Spark version(s); nothing to compare")
    inconclusive = [v for v, r in results.items() if r.status == INCONCLUSIVE]
    if inconclusive:
        return Divergence(False, "inconclusive result on Spark " + ", ".join(inconclusive) +
                          "; the comparison can't be trusted")
    passed = [v for v, r in results.items() if r.status == PASS]
    failed = [v for v, r in results.items() if r.status == FAIL]
    if not failed:
        return Divergence(False, "all tested Spark versions passed")
    if passed:
        return Divergence(True, "Spark " + ", ".join(passed) + " passed but Spark " +
                          ", ".join(failed) + " failed")
    incomplete = [v for v in failed if not results[v].complete]
    if incomplete:
        return Divergence(False, "all tested Spark versions failed, but the failing suites of "
                          "Spark " + ", ".join(incomplete) + " are incomplete")
    if len({frozenset(results[v].keys) for v in failed}) > 1:
        return Divergence(True, "all tested Spark versions failed, on different suites")
    return Divergence(False, "all tested Spark versions failed on the same suites")


# --------------------------------------------------------------------------------------
# Markdown
# --------------------------------------------------------------------------------------

_MD_SPECIAL = re.compile(r"([\\`*_{}\[\]()#+\-.!|<>~@&:\"'=$%^])")


def md_text(value: str) -> str:
    """Escape an untrusted string for Markdown body text (one line, no HTML, no mentions)."""
    value = re.sub(r"[\x00-\x1f\x7f]+", " ", str(value))[:300]
    return _MD_SPECIAL.sub(r"\\\1", value)


def md_code(value: str) -> str:
    """Render an untrusted string as inline code, escaping if it can't be a code span."""
    value = str(value)
    if re.fullmatch(r"[A-Za-z0-9_$./: -]{1,300}", value):
        return f"`{value}`"
    return md_text(value)


def md_url(url: str) -> Optional[str]:
    if re.fullmatch(r"https://github\.com/[A-Za-z0-9_./-]+(?:/job/\d+)?(?:\?pr=\d+)?", url):
        return url
    return None


def _bullets(items: Sequence[str], render=md_code) -> List[str]:
    lines = [f"- {render(i)}" for i in items[:MAX_LISTED]]
    if len(items) > MAX_LISTED:
        lines.append(f"- ... and {len(items) - MAX_LISTED} more")
    return lines


def render_comment(
    author: Optional[str],
    head_sha: str,
    results: Dict[str, VersionResult],
    divergence: Divergence,
    run_url: Optional[str],
) -> str:
    versions = sort_versions(results)
    failed = [v for v in versions if results[v].status == FAIL]
    passed = [v for v in versions if results[v].status == PASS]
    key_versions: Dict[str, Set[str]] = {}
    for v in failed:
        for k in results[v].keys:
            key_versions.setdefault(k, set()).add(v)

    mention = f"@{author}" if author and LOGIN_RE.match(author) else "Hi"
    lines = [MARKER_FORMAT.format(sha=head_sha), ""]
    run_ref = f"[this run]({md_url(run_url)})" if run_url and md_url(run_url) else "this run"
    lines.append(
        f"{mention}, the Delta Spark tests of commit `{head_sha[:12]}` gave different "
        f"results on different Spark versions in {run_ref} "
        f"(tested: {', '.join(md_code(v) for v in versions)}).")
    lines.append("")
    if passed:
        lines.append("Passed on: " + ", ".join(f"Spark {md_code(v)}" for v in passed) + ".")
        lines.append("")
    for v in failed:
        only = sorted(k for k, vs in key_versions.items() if vs == {v})
        lines.append(f"**Failing only on Spark {md_code(v)}:**")
        lines.extend(_bullets(only) if only else ["- (none)"])
        lines.append("")
    if len(failed) == len(versions):
        common = sorted(k for k, vs in key_versions.items() if len(vs) == len(versions))
        if common:
            lines.append("**Failing on all tested versions:**")
            lines.extend(_bullets(common))
            lines.append("")
    partial = sorted((k, vs) for k, vs in key_versions.items()
                     if 1 < len(vs) < len(versions))
    if partial:
        lines.append("**Failing on some, but not all, tested versions:**")
        rendered = [f"{md_code(k)} (Spark {', '.join(md_code(v) for v in sort_versions(vs))})"
                    for k, vs in partial]
        lines.extend(_bullets(rendered, render=lambda s: s))
        lines.append("")
    jobs = []
    for v in failed:
        for s in results[v].failing_jobs():
            url = md_url(s.url)
            jobs.append(f"[{md_text(s.name)}]({url})" if url else md_text(s.name))
    if jobs:
        lines.append("**Failing jobs:**")
        lines.extend(_bullets(jobs, render=lambda s: s))
        lines.append("")
    lines.append(
        f"Because the versions disagree, the `{FULL_CI_LABEL}` label was added to this PR, "
        "so Delta Spark tests now run on every supported Spark version. The label is "
        "sticky: the full matrix runs on every push until a maintainer removes it.")
    lines.append("")
    lines.append(f"<sub>Reason: {md_text(divergence.reason)}. Posted by the Auto full-ci "
                 "workflow (`.github/workflows/auto_full_ci.yaml`).</sub>")
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------------------
# GitHub API
# --------------------------------------------------------------------------------------

class GitHubError(Exception):
    pass


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class GitHubClient:
    """Minimal GitHub REST client (stdlib only).

    Job logs and artifact zips redirect to a pre-signed storage URL. The redirect is
    followed by hand, without the Authorization header, so the token never leaves
    api.github.com.
    """

    def __init__(self, token: Optional[str], api_url: str = "https://api.github.com"):
        if not token:
            raise GitHubError("no GitHub token (set GH_TOKEN)")
        self.token = token
        self.api_url = api_url.rstrip("/")
        self.opener = urllib.request.build_opener(_NoRedirect)

    def _request(self, method: str, path: str, data: Optional[bytes] = None,
                 limit: int = MAX_LOG_BYTES) -> bytes:
        req = urllib.request.Request(
            f"{self.api_url}/{path.lstrip('/')}", data=data, method=method, headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "delta-auto-full-ci",
                **({"Content-Type": "application/json"} if data is not None else {}),
            })
        try:
            with self.opener.open(req, timeout=120) as resp:
                return resp.read(limit)
        except urllib.error.HTTPError as e:
            location = e.headers.get("Location") if e.code in (301, 302, 303, 307, 308) else None
            if not location:
                detail = e.read(500).decode("utf-8", "replace")
                raise GitHubError(f"{method} {path}: HTTP {e.code}: {detail}") from None
            if not location.startswith("https://"):
                raise GitHubError(f"{method} {path}: refusing non-https redirect") from None
            try:
                plain = urllib.request.Request(location, headers={"User-Agent": "delta-auto-full-ci"})
                with urllib.request.urlopen(plain, timeout=300) as resp:
                    return resp.read(limit)
            except urllib.error.URLError as e2:
                raise GitHubError(f"{method} {path}: download failed: {e2}") from None
        except urllib.error.URLError as e:
            raise GitHubError(f"{method} {path}: {e}") from None

    def get_json(self, path: str):
        return json.loads(self._request("GET", path))

    def get_bytes(self, path: str) -> bytes:
        return self._request("GET", path)

    def send_json(self, method: str, path: str, body: Optional[dict] = None):
        data = json.dumps(body if body is not None else {}).encode()
        out = self._request(method, path, data)
        return json.loads(out) if out.strip() else None


def list_paginated(api, path: str, key: Optional[str] = None, max_pages: int = 30) -> List[dict]:
    items = []
    sep = "&" if "?" in path else "?"
    for page in range(1, max_pages + 1):
        data = api.get_json(f"{path}{sep}per_page=100&page={page}")
        chunk = data[key] if key else data
        items.extend(chunk)
        if len(chunk) < 100:
            break
    return items


def fetch_jobs(api, repo: str, run_id: int) -> List[dict]:
    # filter=latest returns the most recent attempt of each job, including jobs that a
    # partial re-run carried over from an earlier attempt.
    jobs = list_paginated(api, f"repos/{repo}/actions/runs/{run_id}/jobs?filter=latest", "jobs")
    return latest_jobs(jobs)


def make_log_fetcher(api, repo: str) -> Callable[[int], Optional[str]]:
    def fetch(job_id: int) -> Optional[str]:
        try:
            data = api.get_bytes(f"repos/{repo}/actions/jobs/{job_id}/logs")
        except GitHubError as e:
            print(f"warning: could not fetch log of job {job_id}: {e}", file=sys.stderr)
            return None
        return data[:MAX_LOG_BYTES].decode("utf-8", "replace")
    return fetch


@dataclass
class PrMatrix:
    pr: int
    head_sha: str
    mode: str
    test_spark_versions: List[str]
    compile_spark_versions: List[str]
    num_shards: Optional[int] = None


def parse_pr_matrix(raw: bytes) -> PrMatrix:
    """Parse and validate the (untrusted) pr-matrix.json content."""
    if len(raw) > 64 * 1024:
        raise ValueError("pr-matrix.json is too large")
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("pr-matrix.json is not an object")
    pr = data.get("pr")
    if not isinstance(pr, int) or isinstance(pr, bool) or pr <= 0:
        raise ValueError("bad pr number")
    head_sha = data.get("head_sha")
    if not isinstance(head_sha, str) or not SHA_RE.match(head_sha):
        raise ValueError("bad head_sha")
    mode = data.get("mode")
    if mode not in ("full", "reduced"):
        raise ValueError("bad mode")
    lists = []
    for key in ("test_spark_versions", "compile_spark_versions"):
        value = data.get(key)
        if (not isinstance(value, list) or len(value) > 32 or
                not all(isinstance(v, str) and VERSION_RE.match(v) for v in value)):
            raise ValueError(f"bad {key}")
        lists.append(sort_versions(value))
    num_shards = data.get("num_shards")
    if num_shards is not None and (not isinstance(num_shards, int) or
                                   isinstance(num_shards, bool) or not 0 < num_shards <= 256):
        raise ValueError("bad num_shards")
    return PrMatrix(pr, head_sha, mode, lists[0], lists[1], num_shards)


def fetch_pr_matrix(api, repo: str, run_id: int, run_attempt: int) -> PrMatrix:
    """Download the pr-matrix artifact of the latest attempt (<= run_attempt) that has one."""
    artifacts = list_paginated(api, f"repos/{repo}/actions/runs/{run_id}/artifacts", "artifacts")
    best = None
    for a in artifacts:
        name = str(a.get("name", ""))
        if not name.startswith(ARTIFACT_PREFIX) or a.get("expired"):
            continue
        suffix = name[len(ARTIFACT_PREFIX):]
        if not suffix.isdigit() or int(suffix) > run_attempt:
            continue
        if best is None or int(suffix) > best[0]:
            best = (int(suffix), a)
    if best is None:
        raise ValueError("no pr-matrix artifact on the run")
    artifact = best[1]
    if int(artifact.get("size_in_bytes") or 0) > MAX_ARTIFACT_BYTES:
        raise ValueError("pr-matrix artifact is too large")
    blob = api.get_bytes(f"repos/{repo}/actions/artifacts/{int(artifact['id'])}/zip")
    if len(blob) > MAX_ARTIFACT_BYTES:
        raise ValueError("pr-matrix artifact is too large")
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        info = zf.getinfo(ARTIFACT_FILE)
        if info.file_size > 64 * 1024:
            raise ValueError("pr-matrix.json is too large")
        return parse_pr_matrix(zf.read(info))


# --------------------------------------------------------------------------------------
# analyze
# --------------------------------------------------------------------------------------

@dataclass
class Analysis:
    label: bool
    reason: str
    pr: Optional[int] = None
    head_sha: Optional[str] = None
    author: Optional[str] = None
    results: Dict[str, VersionResult] = field(default_factory=dict)
    body: Optional[str] = None

    def summary(self) -> dict:
        return {
            "label": self.label,
            "reason": self.reason,
            "pr": self.pr,
            "head_sha": self.head_sha,
            "versions": {
                v: {"status": r.status, "complete": r.complete, "keys": sorted(r.keys),
                    "reason": r.reason,
                    "failing_jobs": [s.name for s in r.failing_jobs()]}
                for v, r in self.results.items()
            },
        }


def _no(reason: str, **kw) -> Analysis:
    return Analysis(False, reason, **kw)


def analyze_run(
    api,
    repo: str,
    run: dict,
    expected_shards: Sequence[str],
    offline: bool = False,
    pr_matrix_loader: Optional[Callable[[], PrMatrix]] = None,
) -> Analysis:
    """Decide whether to add `full-ci` for a completed workflow run.

    `run` is the workflow_run object (from the event, or from the API in offline mode).
    `expected_shards` comes from the trusted (default-branch) spark_test.yaml.
    Offline mode skips the artifact and PR checks and treats the run as reduced, so a
    historical run (including one from before the reduced matrix existed) can be checked.
    """
    if run.get("name") != WORKFLOW_NAME:
        return _no(f"workflow is {run.get('name')!r}, not {WORKFLOW_NAME!r}")
    if run.get("event") != "pull_request":
        return _no(f"event is {run.get('event')!r}, not pull_request")
    if run.get("status") != "completed":
        return _no("run is not completed")
    head_sha = str(run.get("head_sha", ""))
    if not SHA_RE.match(head_sha):
        return _no("run has no valid head SHA")
    run_id = int(run["id"])
    run_attempt = int(run.get("run_attempt") or 1)

    jobs = fetch_jobs(api, repo, run_id)
    matrix = matrix_from_jobs(jobs)

    pr_number = None
    author = None
    if not offline:
        if matrix.mode != "reduced":
            return _no("full-matrix run (no compile jobs)", head_sha=head_sha)
        try:
            pm = (pr_matrix_loader or (lambda: fetch_pr_matrix(api, repo, run_id, run_attempt)))()
        except (ValueError, KeyError, zipfile.BadZipFile, GitHubError) as e:
            return _no(f"pr-matrix artifact unusable: {e}", head_sha=head_sha)
        if pm.head_sha != head_sha:
            return _no("pr-matrix head_sha doesn't match the run", head_sha=head_sha)
        if pm.mode != matrix.mode:
            return _no(f"pr-matrix mode {pm.mode!r} doesn't match the run's jobs "
                       f"({matrix.mode!r})", head_sha=head_sha)
        if (pm.test_spark_versions != matrix.test_versions or
                pm.compile_spark_versions != matrix.compile_versions):
            return _no("pr-matrix versions don't match the run's jobs", head_sha=head_sha)
        if pm.num_shards is not None and pm.num_shards != len(expected_shards):
            return _no(f"pr-matrix num_shards {pm.num_shards} doesn't match the trusted "
                       f"spark_test.yaml ({len(expected_shards)})", head_sha=head_sha)
        listed = [p.get("number") for p in (run.get("pull_requests") or [])]
        if listed and pm.pr not in listed:
            return _no("pr-matrix PR number isn't one of the run's pull requests",
                       head_sha=head_sha)
        try:
            pr = api.get_json(f"repos/{repo}/pulls/{pm.pr}")
        except GitHubError as e:
            return _no(f"could not fetch PR #{pm.pr}: {e}", head_sha=head_sha)
        if pr.get("head", {}).get("sha") != head_sha:
            return _no(f"PR #{pm.pr} head is not the run's head SHA (forged PR number, or "
                       "the PR has new commits)", head_sha=head_sha)
        run_head_repo = (run.get("head_repository") or {}).get("full_name")
        if (pr.get("head", {}).get("repo") or {}).get("full_name") != run_head_repo:
            return _no(f"PR #{pm.pr} head repository doesn't match the run", head_sha=head_sha)
        if (pr.get("base", {}).get("repo") or {}).get("full_name") != repo:
            return _no(f"PR #{pm.pr} is not against {repo}", head_sha=head_sha)
        if pr.get("state") != "open":
            return _no(f"PR #{pm.pr} is not open", head_sha=head_sha)
        state = read_label_state(api, repo, pr, head_sha, run_id, run_attempt)
        skip = state.nothing_to_do()
        if skip:
            return _no(f"PR #{pm.pr}: {skip}", pr=pm.pr, head_sha=head_sha)
        pr_number = pm.pr
        author = (pr.get("user") or {}).get("login")

    results = classify_versions(matrix.test_jobs, make_log_fetcher(api, repo), expected_shards)
    divergence = decide_divergence(results)
    analysis = Analysis(divergence.label, divergence.reason, pr_number, head_sha, author,
                        results)
    if divergence.label:
        analysis.body = render_comment(author, head_sha, results, divergence,
                                       run.get("html_url"))
    return analysis


def _write_outputs(outputs: Dict[str, str]):
    path = os.environ.get("GITHUB_OUTPUT")
    for k, v in outputs.items():
        print(f"{k}={v}")
    if path:
        with open(path, "a") as f:
            for k, v in outputs.items():
                f.write(f"{k}={v}\n")


def cmd_analyze(args, api=None) -> int:
    api = api or GitHubClient(os.environ.get("GH_TOKEN"))
    if not REPO_RE.match(args.repo):
        raise ValueError(f"bad --repo {args.repo!r}")
    if args.run_id:
        run = api.get_json(f"repos/{args.repo}/actions/runs/{int(args.run_id)}")
    else:
        with open(args.event_path or os.environ["GITHUB_EVENT_PATH"]) as f:
            run = json.load(f)["workflow_run"]
    try:
        with open(args.spark_test_workflow) as f:
            expected_shards = expected_shards_from_workflow(f.read())
    except (OSError, ValueError) as e:
        analysis = _no(f"no trusted shard list: {e}")
    else:
        analysis = analyze_run(api, args.repo, run, expected_shards, offline=args.offline)
    print(json.dumps(analysis.summary(), indent=2, sort_keys=True), file=sys.stderr)
    if analysis.body and args.body_file:
        with open(args.body_file, "w") as f:
            f.write(analysis.body)
    if analysis.body and args.offline:
        print("----- comment body -----\n" + analysis.body, file=sys.stderr)
    _write_outputs({
        "should_label": "true" if analysis.label and not args.offline else "false",
        "would_label": "true" if analysis.label else "false",
        "pr": str(analysis.pr or ""),
        "head_sha": analysis.head_sha or "",
        # One line, so it is safe as a job output whatever the body contains.
        "body_b64": base64.b64encode(analysis.body.encode()).decode()
        if analysis.body and not args.offline else "",
    })
    return 0


# --------------------------------------------------------------------------------------
# Label state and apply
# --------------------------------------------------------------------------------------

GITHUB_ACTIONS_BOT = "github-actions[bot]"
PROVENANCE_GITHUB_TOKEN = "github-token"
PROVENANCE_APP = "app"
PROVENANCE_PERSON = "person"
PROVENANCE_UNKNOWN = "unknown"


@dataclass
class LabelState:
    """What has already been done for one PR head SHA (read-only view)."""
    label_present: bool
    # A bot comment with this head SHA's marker exists. It is posted only after the label
    # was added, so it records "full-ci was added for this commit".
    marker_present: bool
    # Who added the present label, from its latest `labeled` event: PROVENANCE_GITHUB_TOKEN,
    # PROVENANCE_APP (another bot), PROVENANCE_PERSON, or PROVENANCE_UNKNOWN when the event
    # can't be established (events API error, or no `labeled` event for the label).
    # None when the label is absent.
    label_provenance: Optional[str] = None
    # The Delta Spark run has an attempt newer than the evaluated one (already re-run).
    newer_attempt: bool = False

    @property
    def label_started_run(self) -> bool:
        """The label's `labeled` event started a workflow run (not made with GITHUB_TOKEN)."""
        return self.label_provenance in (PROVENANCE_APP, PROVENANCE_PERSON)

    def nothing_to_do(self) -> Optional[str]:
        """Return why no further action is needed for this head SHA, or None."""
        if self.marker_present and not self.label_present:
            return (f"{FULL_CI_LABEL} was removed after it was added for this head SHA; "
                    "not adding it again")
        if not self.label_present:
            return None
        if self.label_provenance not in (PROVENANCE_GITHUB_TOKEN, PROVENANCE_APP,
                                         PROVENANCE_PERSON):
            # Without knowing who added the label, a re-run or comment could duplicate a
            # full run or notify for a label this workflow didn't add. Leave it alone.
            return (f"already has the {FULL_CI_LABEL} label, but who added it can't be "
                    "determined; leaving it alone")
        if not self.marker_present and self.label_provenance == PROVENANCE_PERSON:
            return f"already has the {FULL_CI_LABEL} label (added by a person)"
        if self.marker_present and (self.label_started_run or self.newer_attempt):
            return f"already has {FULL_CI_LABEL}, the comment and a full run for this head SHA"
        return None


def read_label_state(api, repo: str, pr: dict, head_sha: str, run_id: int,
                     run_attempt: int) -> LabelState:
    number = int(pr["number"])
    label_present = FULL_CI_LABEL in [lbl.get("name") for lbl in pr.get("labels") or []]
    # Only bot comments count, so a user can't suppress the label by posting the marker.
    marker = MARKER_FORMAT.format(sha=head_sha)
    comments = list_paginated(api, f"repos/{repo}/issues/{number}/comments")
    marker_present = any(
        marker in (c.get("body") or "") and (c.get("user") or {}).get("type") == "Bot"
        for c in comments)
    state = LabelState(label_present, marker_present)
    if label_present:
        state.label_provenance = PROVENANCE_UNKNOWN
        try:
            events = list_paginated(api, f"repos/{repo}/issues/{number}/events")
        except GitHubError as e:
            print(f"warning: could not read PR events: {e}", file=sys.stderr)
            events = []
        labeled = [e for e in events if isinstance(e, dict) and e.get("event") == "labeled"
                   and (e.get("label") or {}).get("name") == FULL_CI_LABEL]
        actor = (labeled[-1].get("actor") or {}) if labeled else {}
        login, actor_type = actor.get("login"), actor.get("type")
        if login == GITHUB_ACTIONS_BOT and actor_type == "Bot":
            state.label_provenance = PROVENANCE_GITHUB_TOKEN
        elif login and actor_type == "Bot":
            state.label_provenance = PROVENANCE_APP
        elif login and actor_type == "User":
            state.label_provenance = PROVENANCE_PERSON
    try:
        run = api.get_json(f"repos/{repo}/actions/runs/{run_id}")
        state.newer_attempt = int(run.get("run_attempt") or 0) > run_attempt
    except GitHubError as e:
        print(f"warning: could not read run {run_id}: {e}", file=sys.stderr)
    return state


def apply_label(api, rerun_api, repo: str, pr_number: int, head_sha: str, body: str,
                run_id: int, run_attempt: int, app_token: bool) -> List[str]:
    """Bring the PR to "labelled, commented once, full run started" for this head SHA.

    Every step is idempotent, so a retry after a partial failure finishes the remaining
    steps: the label is added only if missing, the comment only if this SHA's marker
    comment is missing, and the run is re-run only if no full run was started yet (the
    label was added with GITHUB_TOKEN, and the run has no newer attempt). A present label
    whose source can't be determined, or that a person added, is left alone. The label
    is never removed, and never added again for a SHA whose marker comment exists.
    `api` adds the label and comment (an App token if `app_token`, else GITHUB_TOKEN);
    `rerun_api` (GITHUB_TOKEN) re-runs the run.
    """
    pr = api.get_json(f"repos/{repo}/pulls/{pr_number}")
    if pr.get("head", {}).get("sha") != head_sha or pr.get("state") != "open":
        return ["skipped: PR head moved or PR closed"]
    if MARKER_FORMAT.format(sha=head_sha) not in body:
        raise ValueError("comment body has no marker for this head SHA")
    state = read_label_state(api, repo, pr, head_sha, run_id, run_attempt)
    skip = state.nothing_to_do()
    if skip:
        return ["skipped: " + skip]
    actions = []
    started = state.label_present and state.label_started_run
    if state.label_present:
        actions.append(f"{FULL_CI_LABEL} already present")
    else:
        api.send_json("POST", f"repos/{repo}/issues/{pr_number}/labels",
                      {"labels": [FULL_CI_LABEL]})
        actions.append(f"added {FULL_CI_LABEL}" + (" with the App token" if app_token else ""))
        started = app_token
    if state.marker_present:
        actions.append("comment already posted for this head SHA")
    else:
        api.send_json("POST", f"repos/{repo}/issues/{pr_number}/comments", {"body": body})
        actions.append("posted comment")
    if started:
        actions.append("the labeled event starts the full run; no re-run needed")
    elif state.newer_attempt:
        actions.append(f"run {run_id} already has a newer attempt; no re-run needed")
    else:
        rerun_api.send_json("POST", f"repos/{repo}/actions/runs/{run_id}/rerun")
        actions.append(f"re-ran run {run_id}")
    return actions


def cmd_apply(args, api=None, rerun_api=None) -> int:
    if not REPO_RE.match(args.repo):
        raise ValueError(f"bad --repo {args.repo!r}")
    if not SHA_RE.match(args.head_sha):
        raise ValueError("bad --head-sha")
    api = api or GitHubClient(os.environ.get("GH_TOKEN"))
    rerun_api = rerun_api or GitHubClient(os.environ.get("RERUN_GH_TOKEN"))
    if args.body_b64:
        body = base64.b64decode(args.body_b64, validate=True).decode("utf-8")
    elif args.body_file:
        with open(args.body_file) as f:
            body = f.read()
    else:
        raise ValueError("pass --body-file or --body-b64")
    actions = apply_label(api, rerun_api, args.repo, int(args.pr), args.head_sha, body,
                          int(args.run_id), int(args.run_attempt), args.app_token)
    for a in actions:
        print(a)
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().split("\n\n")[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("analyze", help="decide whether to add full-ci (read-only)")
    a.add_argument("--repo", required=True)
    a.add_argument("--event-path", help="workflow_run event JSON (default $GITHUB_EVENT_PATH)")
    a.add_argument("--run-id", help="analyse this run (fetched from the API) instead")
    a.add_argument("--offline", action="store_true",
                   help="skip artifact/PR checks and treat the run as reduced; never labels")
    a.add_argument("--body-file", help="write the comment body here when labelling")
    a.add_argument("--spark-test-workflow", default=".github/workflows/spark_test.yaml",
                   help="trusted (default-branch) spark_test.yaml with the shard list")
    p = sub.add_parser("apply", help="add full-ci and post the comment")
    p.add_argument("--repo", required=True)
    p.add_argument("--pr", required=True, type=int)
    p.add_argument("--head-sha", required=True)
    p.add_argument("--body-file", help="comment body (Markdown)")
    p.add_argument("--body-b64", help="comment body, base64 (the analyze body_b64 output)")
    p.add_argument("--run-id", required=True, help="the evaluated Delta Spark run")
    p.add_argument("--run-attempt", required=True, type=int,
                   help="the evaluated attempt of --run-id")
    p.add_argument("--app-token", action="store_true",
                   help="$GH_TOKEN is an App token, so adding the label starts a run; "
                        "otherwise the run is re-run with $RERUN_GH_TOKEN")
    args = parser.parse_args(argv)
    if args.cmd == "analyze":
        return cmd_analyze(args)
    return cmd_apply(args)


if __name__ == "__main__":
    sys.exit(main())
