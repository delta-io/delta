#!/usr/bin/env python3
"""
Collect per-suite test durations from CI and write project/test-durations.csv.

Usage:
    # Update test-durations.csv from last 30 successful runs on master (default):
    python3 project/scripts/collect_test_durations.py

    # Use more runs for averaging:
    python3 project/scripts/collect_test_durations.py --last-n-runs 50

    # Time-bound the run to 5 minutes (stops downloading more artifacts after the limit):
    python3 project/scripts/collect_test_durations.py --max-minutes 5

    # Disable the time limit (process all --last-n-runs runs unconditionally):
    python3 project/scripts/collect_test_durations.py --max-minutes 0

Requirements:
    - gh CLI authenticated with access to delta-io/delta
    - Python 3.6+
"""

import argparse
import json
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
DEFAULT_TOP_N = 200
DEFAULT_MAX_MINUTES = 5


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
    """Find the latest N successful workflow run IDs on master."""
    runs_json = run_gh([
        "run", "list", "--repo", REPO,
        "--branch", "master",
        "--workflow", "spark_test.yaml",
        "--status", "success",
        "--limit", str(last_n),
        "--json", "databaseId,headSha,createdAt"
    ])
    runs = json.loads(runs_json)
    if not runs:
        print("No successful runs found on master", file=sys.stderr)
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
        match = re.match(r"test-reports-spark([\d.]+)-shard(\d+)", art["name"])
        if match:
            artifacts.append({
                "id": art["id"],
                "spark_version": match.group(1),
                "shard": int(match.group(2)),
            })

    artifacts.sort(key=lambda a: (a["spark_version"], a["shard"]))
    return artifacts


def download_and_extract(artifact_id, dest_dir):
    """Download and extract a GitHub Actions artifact zip."""
    zip_path = os.path.join(dest_dir, f"{artifact_id}.zip")
    cmd = ["gh", "api", f"repos/{REPO}/actions/artifacts/{artifact_id}/zip"]
    with open(zip_path, 'wb') as f:
        result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE)
    if result.returncode != 0:
        print(f"Error downloading artifact {artifact_id}", file=sys.stderr)
        sys.exit(1)

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
            except ElementTree.ParseError:
                continue

            root = tree.getroot()
            suites = [root] if root.tag == "testsuite" else root.findall("testsuite")
            for suite in suites:
                name = suite.get("name", "")
                if not name:
                    continue
                try:
                    dur = round(float(suite.get("time", "0")) / 60, 2)
                except ValueError:
                    continue
                if name not in durations or dur > durations[name]:
                    durations[name] = dur
    return durations


def collect_from_run(run_id, tmpdir):
    """Collect test durations from a single CI run."""
    artifacts = list_artifacts(run_id)
    if not artifacts:
        print(f"  No artifacts in run {run_id}", file=sys.stderr)
        return {}

    run_durations = {}
    for art in artifacts:
        print(f"    Spark {art['spark_version']}, Shard {art['shard']}...")
        artifact_dir = download_and_extract(art["id"], tmpdir)
        for name, dur in parse_junit_xmls(artifact_dir).items():
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
                        help=f"Number of slowest suites to keep (default: {DEFAULT_TOP_N})")
    parser.add_argument("--max-minutes", type=float, default=DEFAULT_MAX_MINUTES,
                        help=f"Stop downloading after this many minutes (default: {DEFAULT_MAX_MINUTES},"
                             " 0 = no limit)")
    args = parser.parse_args()

    # Fetch run IDs
    print("Finding recent successful runs on master...")
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
            for name, dur in collect_from_run(run_id, tmpdir).items():
                all_durations[name].append(dur)

    # Average and sort
    averaged = {
        name: round(sum(durs) / len(durs), 2)
        for name, durs in all_durations.items()
    }
    sorted_suites = sorted(averaged.items(), key=lambda x: -x[1])[:args.top_n]

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
    print(f"Total duration: {sum(d for _, d in sorted_suites):.1f} min")
    print(f"Slowest: {sorted_suites[0][0]} ({sorted_suites[0][1]} min)")
    print(f"Fastest included: {sorted_suites[-1][0]} ({sorted_suites[-1][1]} min)")


if __name__ == "__main__":
    main()
