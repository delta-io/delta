#!/usr/bin/env python3
"""
Decide which Spark versions the "Delta Spark" workflow (.github/workflows/spark_test.yaml)
runs sharded tests on, and which it only compiles.

Policy:
  * Full matrix (sharded tests on every configured Spark version) when the event is not a
    pull_request, when the PR has the `full-ci` label, when the PR changes build, shim,
    CI or dependency files (see FULL_MATRIX_* below), or when the changed files can't be
    determined (fail safe).
  * Otherwise (reduced PR matrix): sharded tests on the oldest and newest Spark versions,
    and a main+test compile on every version in between.

Most version-specific PR failures show up on the oldest Spark version (APIs missing in
4.0, SQL syntax and exception classes that differ in 4.0/4.1). The newest version covers
newly added APIs and behaviour. The compile job catches shim and API breaks on the
versions in between without running their tests.

Usage (from the workflow):
    python3 project/scripts/spark_ci_matrix.py \
      --event-name pull_request \
      --spark-versions '["4.0", "4.1", "4.2", "4.3"]' \
      --labels '["full-ci"]' \
      --changed-files-file /tmp/changed_files.tsv \
      --expected-changed-files 12

  The changed-files file has one line per PR file: "<filename>" or
  "<filename>\t<previous_filename>" (for renames). Pass --changed-files-error to force the
  fail-safe path. Outputs (key=value) go to $GITHUB_OUTPUT when it is set, and to stdout.
"""

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Sequence

FULL_CI_LABEL = "full-ci"

# Exact repo-relative paths that force the full matrix.
FULL_MATRIX_FILES = {
    "build.sbt",
    "version.sbt",
    "run-tests.py",
    ".github/workflows/spark_test.yaml",
    "python/environment.yml",
}

# Repo-relative directory prefixes that force the full matrix.
FULL_MATRIX_PREFIXES = (
    "project/",
    "build/",
    ".github/actions/",
)

# File names (anywhere in the repo) that force the full matrix.
FULL_MATRIX_BASENAMES = {
    "setup.py",
    "pyproject.toml",
}

# Directory names (any path segment except the file name) that force the full matrix:
# Spark version-specific source roots.
FULL_MATRIX_DIR_PATTERNS = (
    re.compile(r"^scala-shims$"),
    re.compile(r"^java-shims$"),
    re.compile(r"^scala-spark-.+$"),
    re.compile(r"^java-spark-.+$"),
    re.compile(r"^spark-master$"),
)

# Other full-matrix path patterns (matched against the full repo-relative path).
FULL_MATRIX_PATH_PATTERNS = (
    # python/**/requirements*.txt
    re.compile(r"^python/(.+/)?requirements[^/]*\.txt$"),
    # Lock files anywhere: foo.lock, uv.lock, pnpm-lock.yaml, package-lock.json,
    # .terraform.lock.hcl, ...
    re.compile(r"(^|/)[^/]*\.lock$"),
    re.compile(r"(^|/)[^/]*\.lock\.[^/]+$"),
    re.compile(r"(^|/)[^/]*-lock\.(json|yaml|yml)$"),
)


@dataclass
class Decision:
    mode: str  # "full" or "reduced"
    reason: str
    test_spark_versions: List[str]
    compile_spark_versions: List[str]
    full_matrix_files: List[str] = field(default_factory=list)

    def outputs(self):
        return {
            "mode": self.mode,
            "reason": self.reason,
            "test_spark_versions": json.dumps(self.test_spark_versions),
            "compile_spark_versions": json.dumps(self.compile_spark_versions),
        }


def full_matrix_trigger(path: str) -> bool:
    """Return True if a change to `path` (repo-relative) requires the full matrix."""
    path = path.strip()
    if path.startswith("./"):
        path = path[2:]
    if not path:
        return False
    if path in FULL_MATRIX_FILES:
        return True
    if path.startswith(FULL_MATRIX_PREFIXES):
        return True
    segments = path.split("/")
    if segments[-1] in FULL_MATRIX_BASENAMES:
        return True
    for segment in segments[:-1]:
        if any(p.match(segment) for p in FULL_MATRIX_DIR_PATTERNS):
            return True
    return any(p.search(path) for p in FULL_MATRIX_PATH_PATTERNS)


def _version_sort_key(version: str):
    """Sort "4.0" < "4.1" < "4.10" < "master" (master is the newest)."""
    if version == "master":
        return (1, ())
    parts = []
    for part in version.split("."):
        parts.append(int(part) if part.isdigit() else -1)
    return (0, tuple(parts))


def sort_spark_versions(versions: Iterable[str]) -> List[str]:
    return sorted(dict.fromkeys(versions), key=_version_sort_key)


def split_versions(versions: Sequence[str]):
    """Split versions into (test, compile): oldest and newest are tested, the rest compiled."""
    ordered = sort_spark_versions(versions)
    if len(ordered) <= 2:
        return ordered, []
    return [ordered[0], ordered[-1]], ordered[1:-1]


def parse_changed_files(lines: Iterable[str]) -> List[List[str]]:
    """Parse "<filename>[\\t<previous_filename>]" lines into per-file path lists."""
    entries = []
    for line in lines:
        line = line.rstrip("\n")
        if not line.strip():
            continue
        entries.append([p for p in line.split("\t") if p])
    return entries


def decide(
    event_name: str,
    spark_versions: Sequence[str],
    labels: Optional[Sequence[str]] = None,
    changed_files: Optional[List[List[str]]] = None,
    expected_changed_files: Optional[int] = None,
    detection_error: Optional[str] = None,
) -> Decision:
    """Compute the matrix decision.

    `changed_files` is a list of per-file path lists (current name, then the previous name
    for renames). `None` means change detection failed.
    """
    all_versions = sort_spark_versions(spark_versions)

    def full(reason, files=None):
        return Decision("full", reason, all_versions, [], files or [])

    if event_name != "pull_request":
        return full(f"event '{event_name}' is not pull_request")
    if FULL_CI_LABEL in (labels or []):
        return full(f"PR has the '{FULL_CI_LABEL}' label")
    if detection_error:
        return full(f"change detection failed: {detection_error}")
    if changed_files is None:
        return full("change detection failed: no changed-file list")
    if expected_changed_files is not None and len(changed_files) != expected_changed_files:
        return full(
            "change detection failed: got %d changed files, PR reports %d"
            % (len(changed_files), expected_changed_files))
    if not changed_files:
        return full("change detection failed: empty changed-file list")

    triggers = sorted({p for entry in changed_files for p in entry if full_matrix_trigger(p)})
    if triggers:
        shown = ", ".join(triggers[:5]) + (", ..." if len(triggers) > 5 else "")
        return full(f"PR changes build/shim/CI/dependency files: {shown}", triggers)

    test_versions, compile_versions = split_versions(all_versions)
    return Decision(
        "reduced",
        "PR changes no full-matrix files; testing oldest and newest Spark versions",
        test_versions,
        compile_versions,
    )


def write_outputs(outputs, github_output_path: Optional[str]):
    lines = [f"{k}={v}" for k, v in outputs.items()]
    for line in lines:
        print(line)
    if github_output_path:
        with open(github_output_path, "a") as f:
            for line in lines:
                f.write(line + "\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--spark-versions", required=True,
                        help="JSON array of non-source-build Spark versions")
    parser.add_argument("--labels", default="[]", help="JSON array of PR label names")
    parser.add_argument("--changed-files-file",
                        help="File with one '<filename>[\\t<previous_filename>]' line per file")
    parser.add_argument("--expected-changed-files", default="",
                        help="Changed-file count reported by the PR (for a completeness check)")
    parser.add_argument("--changed-files-error", default="",
                        help="Non-empty when fetching the changed files failed")
    args = parser.parse_args(argv)

    spark_versions = json.loads(args.spark_versions)
    if not isinstance(spark_versions, list) or not spark_versions:
        raise ValueError(f"--spark-versions must be a non-empty JSON array: {args.spark_versions}")

    detection_error = args.changed_files_error or None
    labels = []
    changed_files = None
    expected = None
    if args.event_name == "pull_request":
        try:
            labels = json.loads(args.labels or "[]")
        except ValueError as e:
            # Labels are only used to widen the matrix; unparsable labels fail safe to full.
            detection_error = detection_error or f"could not parse labels: {e}"
        if args.expected_changed_files:
            try:
                expected = int(args.expected_changed_files)
            except ValueError:
                detection_error = detection_error or (
                    f"bad changed-file count {args.expected_changed_files!r}")
        if args.changed_files_file:
            try:
                with open(args.changed_files_file) as f:
                    changed_files = parse_changed_files(f)
            except OSError as e:
                detection_error = detection_error or f"could not read changed files: {e}"

    decision = decide(
        args.event_name, spark_versions, labels, changed_files, expected, detection_error)
    write_outputs(decision.outputs(), os.environ.get("GITHUB_OUTPUT"))
    if decision.full_matrix_files:
        print("Full-matrix files:", file=sys.stderr)
        for path in decision.full_matrix_files:
            print(f"  {path}", file=sys.stderr)


if __name__ == "__main__":
    main()
