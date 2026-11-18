#!/usr/bin/env python3
"""
Collect per-suite test durations from CI and write project/test-durations.csv.

Usage:
    # Update all suite timings from complete Spark versions in recent master runs:
    python3 project/scripts/collect_test_durations.py

    # Use more runs for averaging:
    python3 project/scripts/collect_test_durations.py --last-n-runs 50

    # Stop starting new artifact downloads after 5 minutes:
    python3 project/scripts/collect_test_durations.py --max-minutes 5

    # Disable the time limit (process all --last-n-runs runs unconditionally):
    python3 project/scripts/collect_test_durations.py --max-minutes 0

Requirements:
    - gh CLI authenticated with access to delta-io/delta
    - Python 3.6+
"""

import argparse
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import time
import zipfile
from collections import defaultdict
from xml.etree import ElementTree


REPO = "delta-io/delta"
CSV_FILE = "project/test-durations.csv"
DEFAULT_LAST_N_RUNS = 30
DEFAULT_TOP_N = 0
DEFAULT_MAX_MINUTES = 5
SHARDS_PER_SPARK_VERSION = 8


def run_gh(args):
    """Run a gh CLI command and return stdout."""
    cmd = ["gh"] + args
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        print(f"Error running: {' '.join(cmd)}", file=sys.stderr)
        print(result.stderr.decode(), file=sys.stderr)
        sys.exit(1)
    return result.stdout.decode()


def get_run_ids(last_n):
    """Find recent completed workflows, including runs with successful shard jobs."""
    runs_json = run_gh([
        "run", "list", "--repo", REPO,
        "--branch", "master",
        "--workflow", "spark_test.yaml",
        "--status", "completed",
        "--limit", str(last_n),
        "--json", "databaseId,headSha,createdAt"
    ])
    runs = json.loads(runs_json)
    if not runs:
        print("No completed runs found on master", file=sys.stderr)
        sys.exit(1)

    for run in runs:
        print(f"  Run {run['databaseId']} (commit: {run['headSha'][:8]}, "
              f"created: {run['createdAt']})")

    return [run["databaseId"] for run in runs]


def list_artifacts(run_id):
    """List test-report artifacts from a workflow run."""
    artifacts_json = run_gh([
        "api", f"repos/{REPO}/actions/runs/{run_id}/artifacts",
        "--paginate", "--jq", ".artifacts[]"
    ])

    artifacts = []
    for line in artifacts_json.strip().split('\n'):
        if not line.strip():
            continue
        art = json.loads(line)
        match = re.fullmatch(r"test-reports-spark([\d.]+)-shard(\d+)", art["name"])
        if match and not art.get("expired", False):
            artifacts.append({
                "id": art["id"],
                "spark_version": match.group(1),
                "shard": int(match.group(2)),
            })

    artifacts.sort(key=lambda a: (a["spark_version"], a["shard"]))
    return artifacts


def get_successful_shards(run_id):
    """Return successful Spark shard jobs from the latest run attempt."""
    jobs_json = run_gh([
        "api", f"repos/{REPO}/actions/runs/{run_id}/jobs",
        "--paginate", "--jq", ".jobs[]"
    ])
    shards = defaultdict(set)
    for line in jobs_json.splitlines():
        if not line.strip():
            continue
        job = json.loads(line)
        match = re.search(r"Spark ([\d.]+), .*Shard (\d+)$", job["name"])
        if match and job["conclusion"] == "success":
            shards[match.group(1)].add(int(match.group(2)))
    return shards


def download_and_extract(artifact_id, dest_dir):
    """Download and extract a GitHub Actions artifact zip."""
    zip_path = os.path.join(dest_dir, f"{artifact_id}.zip")
    cmd = ["gh", "api", f"repos/{REPO}/actions/artifacts/{artifact_id}/zip"]
    with open(zip_path, 'wb') as f:
        result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE)
    if result.returncode != 0:
        raise ValueError(f"Could not download artifact {artifact_id}")

    extract_dir = os.path.join(dest_dir, str(artifact_id))
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(extract_dir)
    return extract_dir


def parse_junit_xmls(directory):
    """Parse all JUnit XML files in a directory. Returns {suite_name: duration_minutes}."""
    durations = {}
    for dirpath, _, filenames in os.walk(directory):
        for fname in filenames:
            if not fname.endswith('.xml'):
                continue
            try:
                tree = ElementTree.parse(os.path.join(dirpath, fname))
            except ElementTree.ParseError as error:
                raise ValueError(f"Invalid test report: {fname}") from error

            root = tree.getroot()
            suites = [root] if root.tag == "testsuite" else root.findall("testsuite")
            for suite in suites:
                name = suite.get("name", "")
                if not name:
                    continue
                if int(suite.get("failures", "0")) or int(suite.get("errors", "0")):
                    raise ValueError(f"Failed tests in {fname}")
                seconds = float(suite.get("time", "0"))
                # Skipped tests use -0.001 seconds and can make a short suite's total negative.
                test_cases = suite.findall("testcase")
                if seconds < 0 and any(case.find("skipped") is not None for case in test_cases):
                    seconds = sum(float(case.get("time", "0")) for case in test_cases
                                  if case.find("skipped") is None)
                if not math.isfinite(seconds) or seconds < 0:
                    raise ValueError(f"Invalid test duration in {fname}")
                dur = round(seconds / 60, 2)
                if name not in durations or dur > durations[name]:
                    durations[name] = dur
    if not durations:
        raise ValueError("No test suites found in the artifact")
    return durations


def collect_from_run(run_id, tmpdir, deadline=None):
    """Collect only complete, successful Spark versions from a CI run."""
    artifacts = list_artifacts(run_id)
    if not artifacts:
        print(f"  No artifacts in run {run_id}", file=sys.stderr)
        return {}

    successful_shards = get_successful_shards(run_id)
    by_version = defaultdict(list)
    for art in artifacts:
        by_version[art["spark_version"]].append(art)

    expected_shards = set(range(SHARDS_PER_SPARK_VERSION))
    run_durations = {}
    for version, version_artifacts in sorted(by_version.items()):
        artifact_shards = {art["shard"] for art in version_artifacts}
        if (successful_shards.get(version, set()) != expected_shards or
                artifact_shards != expected_shards or
                len(version_artifacts) != SHARDS_PER_SPARK_VERSION):
            print(f"    Skipping Spark {version}: incomplete or failed shards")
            continue

        version_durations = {}
        try:
            for art in version_artifacts:
                if deadline is not None and time.monotonic() >= deadline:
                    print(f"    Time limit reached; skipping incomplete Spark {version}")
                    return run_durations
                print(f"    Spark {version}, Shard {art['shard']}...")
                artifact_dir = download_and_extract(art["id"], tmpdir)
                for name, dur in parse_junit_xmls(artifact_dir).items():
                    version_durations[name] = max(version_durations.get(name, 0), dur)
        except (ValueError, zipfile.BadZipFile) as error:
            print(f"    Skipping Spark {version}: {error}", file=sys.stderr)
            continue

        print(f"    Spark {version}: {len(version_durations)} suites from all "
              f"{SHARDS_PER_SPARK_VERSION} shards")
        for name, dur in version_durations.items():
            if name not in run_durations or dur > run_durations[name]:
                run_durations[name] = dur

    return run_durations


def main():
    parser = argparse.ArgumentParser(
        description="Collect test durations from CI and update project/test-durations.csv"
    )
    parser.add_argument("--last-n-runs", type=int, default=DEFAULT_LAST_N_RUNS,
                        help=f"Number of runs to average (default: {DEFAULT_LAST_N_RUNS})")
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N,
                        help="Number of slowest suites to keep (default: 0 = all suites)")
    parser.add_argument("--max-minutes", type=float, default=DEFAULT_MAX_MINUTES,
                        help=f"Stop downloading after this many minutes (default: {DEFAULT_MAX_MINUTES},"
                             " 0 = no limit)")
    args = parser.parse_args()

    # Fetch run IDs
    print("Finding recent completed runs on master...")
    run_ids = get_run_ids(args.last_n_runs)

    deadline = time.monotonic() + args.max_minutes * 60 if args.max_minutes > 0 else None

    # Collect durations from each run
    all_durations = defaultdict(list)
    with tempfile.TemporaryDirectory() as tmpdir:
        for i, run_id in enumerate(run_ids):
            if deadline is not None and time.monotonic() >= deadline:
                print(f"\nTime limit reached after {i} run(s); stopping early.")
                break
            print(f"\nRun {i + 1}/{len(run_ids)}: {run_id}")
            for name, dur in collect_from_run(run_id, tmpdir, deadline).items():
                all_durations[name].append(dur)

    # Average and sort
    averaged = {
        name: round(sum(durs) / len(durs), 2)
        for name, durs in all_durations.items()
    }
    sorted_suites = sorted(averaged.items(), key=lambda x: (-x[1], x[0]))
    if args.top_n > 0:
        sorted_suites = sorted_suites[:args.top_n]

    if not sorted_suites:
        print("\nNo test-report artifacts found. Has the JUnit XML upload been enabled on master?")
        print(f"{CSV_FILE} was NOT modified.")
        sys.exit(1)

    # Write CSV
    with open(CSV_FILE, 'w') as f:
        f.write("suite_name,duration_minutes\n")
        for name, dur in sorted_suites:
            f.write(f"{name},{dur}\n")

    print(f"\nWrote {len(sorted_suites)} suites to {CSV_FILE}")
    sample_counts = [len(all_durations[name]) for name, _ in sorted_suites]
    print(f"Samples per suite: {min(sample_counts)} to {max(sample_counts)} run(s)")
    print(f"Total duration: {sum(d for _, d in sorted_suites):.1f} min")
    print(f"Slowest: {sorted_suites[0][0]} ({sorted_suites[0][1]} min)")
    print(f"Fastest included: {sorted_suites[-1][0]} ({sorted_suites[-1][1]} min)")


if __name__ == "__main__":
    main()
