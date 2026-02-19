#!/usr/bin/env python3
"""
Collect per-suite test durations from JUnit XML test reports uploaded as
GitHub Actions artifacts and optionally update TestParallelization.scala.

Usage:
    # Rebalance shards (fetches last 5 successful runs on master, updates file):
    python3 project/scripts/collect_test_durations.py --update-file

    # Dry run — print durations and shard balance without modifying any files:
    python3 project/scripts/collect_test_durations.py

    # Use a different branch or number of runs:
    python3 project/scripts/collect_test_durations.py --branch my-branch --last-n-runs 10

    # Analyze local test reports (used by CI after downloading artifacts):
    python3 project/scripts/collect_test_durations.py --local-dir ./test-reports

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
import zipfile
from collections import defaultdict
from xml.etree import ElementTree


REPO = "delta-io/delta"
TEST_PARALLELIZATION_FILE = "project/TestParallelization.scala"
DEFAULT_LAST_N_RUNS = 5
DEFAULT_TOP_N = 100


def run_gh(args, check=True):
    """Run a gh CLI command and return stdout."""
    cmd = ["gh"] + args
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if check and result.returncode != 0:
        print(f"Error running: {' '.join(cmd)}", file=sys.stderr)
        print(result.stderr.decode(), file=sys.stderr)
        sys.exit(1)
    return result.stdout.decode()


def run_gh_binary(args, dest_path, check=True):
    """Run a gh CLI command and save binary stdout to a file."""
    cmd = ["gh"] + args
    with open(dest_path, 'wb') as f:
        result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE)
    if check and result.returncode != 0:
        print(f"Error running: {' '.join(cmd)}", file=sys.stderr)
        print(result.stderr.decode(), file=sys.stderr)
        sys.exit(1)


def get_run_ids(branch, last_n):
    """Find the latest N successful workflow run IDs on a branch."""
    print(f"Using branch: {branch}")
    runs_json = run_gh([
        "run", "list", "--repo", REPO,
        "--branch", branch,
        "--workflow", "spark_test.yaml",
        "--status", "success",
        "--limit", str(last_n),
        "--json", "databaseId,headSha,createdAt"
    ])
    runs = json.loads(runs_json)
    if not runs:
        print(f"No successful Delta Spark runs found on branch '{branch}'",
              file=sys.stderr)
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
        match = re.match(
            r"test-reports-spark([\d.]+)-shard(\d+)",
            art["name"]
        )
        if match:
            artifacts.append({
                "id": art["id"],
                "name": art["name"],
                "spark_version": match.group(1),
                "shard": int(match.group(2)),
            })

    artifacts.sort(key=lambda a: (a["spark_version"], a["shard"]))
    return artifacts


def download_artifact(artifact_id, dest_dir):
    """Download and extract a GitHub Actions artifact zip."""
    zip_path = os.path.join(dest_dir, f"artifact_{artifact_id}.zip")
    run_gh_binary(
        ["api", f"repos/{REPO}/actions/artifacts/{artifact_id}/zip"],
        zip_path
    )

    extract_dir = os.path.join(dest_dir, str(artifact_id))
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(extract_dir)

    return extract_dir


def parse_junit_xml(xml_path):
    """Parse a JUnit XML file and return list of (suite_name, duration_seconds)."""
    try:
        tree = ElementTree.parse(xml_path)
    except ElementTree.ParseError as e:
        print(f"  Warning: failed to parse {xml_path}: {e}", file=sys.stderr)
        return None

    root = tree.getroot()
    if root.tag == "testsuite":
        suites = [root]
    elif root.tag == "testsuites":
        suites = root.findall("testsuite")
    else:
        return None

    results = []
    for suite in suites:
        name = suite.get("name", "")
        try:
            duration_sec = float(suite.get("time", "0"))
        except ValueError:
            duration_sec = 0.0
        if name:
            results.append((name, duration_sec))

    return results


def parse_artifact_dir(artifact_dir):
    """Parse all JUnit XML files in a directory. Returns (durations_dict, xml_count)."""
    durations = {}
    xml_count = 0
    for dirpath, _, filenames in os.walk(artifact_dir):
        for fname in filenames:
            if not fname.endswith('.xml'):
                continue
            xml_count += 1
            results = parse_junit_xml(os.path.join(dirpath, fname))
            if not results:
                continue
            for suite_name, duration_sec in results:
                dur_min = round(duration_sec / 60, 2)
                if suite_name not in durations or dur_min > durations[suite_name]:
                    durations[suite_name] = dur_min

    return durations, xml_count


def collect_durations_from_local_dir(local_dir, spark_version_filter):
    """Collect test durations from local artifact directories on disk."""
    all_durations = {}
    shard_durations = defaultdict(float)

    for entry in sorted(os.listdir(local_dir)):
        entry_path = os.path.join(local_dir, entry)
        if not os.path.isdir(entry_path):
            continue

        match = re.match(r"test-reports-spark([\d.]+)-shard(\d+)", entry)
        if not match:
            continue

        spark_version = match.group(1)
        shard = int(match.group(2))

        if spark_version_filter and spark_version != spark_version_filter:
            continue

        print(f"  Parsing {entry}...")
        durations, xml_count = parse_artifact_dir(entry_path)
        print(f"    -> {xml_count} XML files, {len(durations)} suites")

        shard_total = 0.0
        for suite_name, dur in durations.items():
            if suite_name not in all_durations or dur > all_durations[suite_name]:
                all_durations[suite_name] = dur
            shard_total += dur
        shard_durations[(spark_version, shard)] = shard_total

    if shard_durations:
        print(f"\n--- Actual per-shard durations (this run) ---")
        for (sv, shard), total in sorted(shard_durations.items()):
            print(f"  Spark {sv}, Shard {shard}: {total:.1f} min")

    return all_durations


def collect_durations_from_run(run_id, spark_version_filter, tmpdir):
    """Collect test durations from a single workflow run via GitHub API."""
    artifacts = list_artifacts(run_id)
    if not artifacts:
        print(f"  No test-report artifacts found in run {run_id}", file=sys.stderr)
        return {}

    if spark_version_filter:
        artifacts = [a for a in artifacts
                     if a["spark_version"] == spark_version_filter]

    run_durations = {}
    for art in artifacts:
        label = f"Spark {art['spark_version']}, Shard {art['shard']}"
        print(f"    Downloading {label} (artifact {art['id']})...")
        artifact_dir = download_artifact(art["id"], tmpdir)
        durations, xml_count = parse_artifact_dir(artifact_dir)
        print(f"      -> {xml_count} XML files, {len(durations)} suites")

        for suite_name, dur in durations.items():
            if suite_name not in run_durations or dur > run_durations[suite_name]:
                run_durations[suite_name] = dur

    return run_durations


def update_test_parallelization_file(sorted_suites, filepath):
    """Update the TOP_N_HIGH_DURATION_TEST_SUITES list in TestParallelization.scala."""
    with open(filepath, 'r') as f:
        content = f.read()

    pattern = (
        r'(  /\*\*.*?slowest test suites.*?\*/\n'
        r'  val TOP_\w+_HIGH_DURATION_TEST_SUITES: List\[\(String, Double\)\] = List\()\n'
        r'.*?'
        r'(  \))'
    )
    match = re.search(pattern, content, re.DOTALL)
    if not match:
        print("Error: Could not find TOP_N_HIGH_DURATION_TEST_SUITES in "
              f"{filepath}", file=sys.stderr)
        sys.exit(1)

    n = len(sorted_suites)
    lines = []
    lines.append(f"  /** {n} slowest test suites and their durations (in minutes). */")
    lines.append(
        f"  val TOP_{n}_HIGH_DURATION_TEST_SUITES: "
        f"List[(String, Double)] = List(")
    for i, (name, dur) in enumerate(sorted_suites):
        comma = "," if i < n - 1 else ""
        lines.append(f'    ("{name}", {dur}){comma}')
    lines.append("  )")

    new_content = content[:match.start()] + "\n".join(lines) + content[match.end():]

    with open(filepath, 'w') as f:
        f.write(new_content)

    print(f"\nUpdated {filepath} with {n} test suites.")


def print_shard_balance_analysis(sorted_suites, num_shards, num_groups):
    """Simulate the greedy assignment and print shard balance analysis."""
    group_durations = [[0.0] * num_groups for _ in range(num_shards)]
    shard_durations = [0.0] * num_shards
    group_counts = [[0] * num_groups for _ in range(num_shards)]

    for name, duration in sorted_suites:
        best_shard, best_group = -1, -1
        min_group_dur = float('inf')
        min_shard_dur = float('inf')

        for s in range(num_shards):
            for g in range(num_groups):
                gd = group_durations[s][g]
                sd = shard_durations[s]
                if gd < min_group_dur or (gd == min_group_dur and sd < min_shard_dur):
                    min_group_dur = gd
                    min_shard_dur = sd
                    best_shard = s
                    best_group = g

        group_durations[best_shard][best_group] += duration
        shard_durations[best_shard] += duration
        group_counts[best_shard][best_group] += 1

    print(f"\n{'=' * 80}")
    print(f"SHARD BALANCE ANALYSIS ({num_shards} shards, {num_groups} groups/shard)")
    print(f"{'=' * 80}\n")

    for s in range(num_shards):
        print(f"  Shard {s}: {shard_durations[s]:.1f} min total")
        for g in range(num_groups):
            print(f"    Group {g}: {group_durations[s][g]:.1f} min "
                  f"({group_counts[s][g]} suites)")

    max_shard = max(shard_durations)
    min_shard = min(shard_durations)
    print(f"\n  Max shard: {max_shard:.1f} min")
    print(f"  Min shard: {min_shard:.1f} min")
    if max_shard > 0:
        print(f"  Imbalance: {max_shard - min_shard:.1f} min "
              f"({(max_shard - min_shard) / max_shard * 100:.0f}%)")


def main():
    parser = argparse.ArgumentParser(
        description="Collect test suite durations from CI and rebalance shards"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--branch", type=str,
                       help="Branch name (default: master)")
    group.add_argument("--local-dir", type=str,
                       help="Local directory with downloaded test-report artifacts")
    parser.add_argument(
        "--last-n-runs", type=int, default=DEFAULT_LAST_N_RUNS,
        help=f"Number of recent runs to average (default: {DEFAULT_LAST_N_RUNS})")
    parser.add_argument(
        "--spark-version", type=str, default=None,
        help="Only process a specific Spark version (e.g., '4.0')")
    parser.add_argument(
        "--top-n", type=int, default=DEFAULT_TOP_N,
        help=f"Number of slowest suites to include (default: {DEFAULT_TOP_N})")
    parser.add_argument(
        "--update-file", action="store_true",
        help="Directly update TestParallelization.scala with the new durations")
    parser.add_argument(
        "--num-shards", type=int, default=4,
        help="Number of shards for balance analysis (default: 4)")
    parser.add_argument(
        "--num-groups", type=int, default=4,
        help="Number of parallel groups per shard (default: 4)")
    args = parser.parse_args()

    # Default to master if no source specified
    if not args.branch and not args.local_dir:
        args.branch = "master"

    # Collect durations
    if args.local_dir:
        print(f"Parsing local test reports from {args.local_dir}...")
        avg_durations = collect_durations_from_local_dir(
            args.local_dir, args.spark_version)
        num_sources = 1
    else:
        run_ids = get_run_ids(args.branch, args.last_n_runs)
        num_sources = len(run_ids)
        print(f"\nCollecting durations from {num_sources} run(s)...")

        all_run_durations = defaultdict(list)
        with tempfile.TemporaryDirectory() as tmpdir:
            for i, run_id in enumerate(run_ids):
                print(f"\n--- Run {i + 1}/{num_sources}: {run_id} ---")
                run_durations = collect_durations_from_run(
                    run_id, args.spark_version, tmpdir)
                for suite_name, dur in run_durations.items():
                    all_run_durations[suite_name].append(dur)

        avg_durations = {
            name: round(sum(durs) / len(durs), 2)
            for name, durs in all_run_durations.items()
        }

    # Sort and filter
    sorted_suites = sorted(avg_durations.items(), key=lambda x: -x[1])
    sorted_suites = sorted_suites[:args.top_n]

    total_suites = len(sorted_suites)
    total_duration = sum(d for _, d in sorted_suites)

    # Print results
    print(f"\n{'=' * 100}")
    print(f"RESULTS: {total_suites} suites, {total_duration:.1f} min total "
          f"(averaged across {num_sources} run(s))")
    print(f"{'=' * 100}\n")

    print(f"{'#':<5} {'Suite':<90} {'Avg (min)':>10}")
    print("-" * 107)
    for i, (name, dur) in enumerate(sorted_suites, 1):
        print(f"{i:<5} {name:<90} {dur:>8.2f}m")

    # Shard balance analysis
    print_shard_balance_analysis(sorted_suites, args.num_shards, args.num_groups)

    # Optionally update the file
    if args.update_file:
        update_test_parallelization_file(sorted_suites, TEST_PARALLELIZATION_FILE)
        print("\nDone! Review the changes with: git diff project/TestParallelization.scala")


if __name__ == "__main__":
    main()
