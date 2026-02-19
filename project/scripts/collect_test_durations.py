#!/usr/bin/env python3
"""
Collect per-suite test durations from JUnit XML test reports uploaded as
GitHub Actions artifacts and optionally update TestParallelization.scala.

Downloads test-report artifacts from Delta Spark CI runs and parses the
JUnit XML files to extract exact per-suite durations. Can average across
multiple CI runs for stable estimates.

Usage:
    # Rebalance shards using the latest 5 successful runs on master:
    python3 project/scripts/collect_test_durations.py --branch master --update-file

    # Preview what would change (dry run):
    python3 project/scripts/collect_test_durations.py --branch master --top-n 100 --output-scala

    # Use a specific number of runs for averaging:
    python3 project/scripts/collect_test_durations.py --branch master --last-n-runs 10

    # Use a single workflow run ID:
    python3 project/scripts/collect_test_durations.py --run-id 22150233443

    # Use a specific PR (single run):
    python3 project/scripts/collect_test_durations.py --pr 6069

    # Filter by Spark version:
    python3 project/scripts/collect_test_durations.py --branch master --spark-version 4.0

    # Analyze local test reports (used by CI after downloading artifacts):
    python3 project/scripts/collect_test_durations.py --local-dir ./test-reports

Requirements:
    - gh CLI authenticated with access to delta-io/delta
    - Python 3.6+
    - JUnit XML test reports must be uploaded as artifacts by CI
      (enabled via Test / testOptions += Tests.Argument("-u", "target/test-reports")
       in build.sbt and the upload-artifact step in spark_test.yaml)
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


def get_run_ids(args):
    """Resolve one or more workflow run IDs from CLI arguments."""
    if args.run_id:
        return [args.run_id]

    if args.pr:
        pr_json = run_gh([
            "pr", "view", str(args.pr), "--repo", REPO,
            "--json", "headRefName"
        ])
        branch = json.loads(pr_json)["headRefName"]
        print(f"PR #{args.pr} -> branch: {branch}")
    else:
        branch = args.branch
        print(f"Using branch: {branch}")

    last_n = args.last_n_runs

    # Find the latest N "Delta Spark" workflow runs on this branch
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
        # Match artifact names like "test-reports-spark4.0-shard3"
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
    """
    Parse a JUnit XML file and return list of (suite_name, duration_seconds).

    JUnit XML format (ScalaTest):
        <testsuite name="org.apache.spark.sql.delta.FooSuite"
                   tests="42" time="123.456" ...>
            <testcase name="test name" time="1.23" .../>
            ...
        </testsuite>
    """
    try:
        tree = ElementTree.parse(xml_path)
    except ElementTree.ParseError as e:
        print(f"  Warning: failed to parse {xml_path}: {e}", file=sys.stderr)
        return None

    root = tree.getroot()

    # Handle both <testsuite> root and <testsuites><testsuite> root
    if root.tag == "testsuite":
        suites = [root]
    elif root.tag == "testsuites":
        suites = root.findall("testsuite")
    else:
        return None

    results = []
    for suite in suites:
        name = suite.get("name", "")
        time_str = suite.get("time", "0")
        try:
            duration_sec = float(time_str)
        except ValueError:
            duration_sec = 0.0

        if name:
            results.append((name, duration_sec))

    return results


def parse_artifact_dir(artifact_dir):
    """Parse all JUnit XML files in an extracted artifact directory."""
    durations = {}  # suite_name -> duration_minutes

    xml_count = 0
    for dirpath, _, filenames in os.walk(artifact_dir):
        for fname in filenames:
            if not fname.endswith('.xml'):
                continue
            xml_path = os.path.join(dirpath, fname)
            xml_count += 1

            results = parse_junit_xml(xml_path)
            if not results:
                continue

            for suite_name, duration_sec in results:
                dur_min = round(duration_sec / 60, 2)
                # Keep the longer duration if a suite appears multiple times
                if suite_name not in durations or dur_min > durations[suite_name]:
                    durations[suite_name] = dur_min

    return durations, xml_count


def collect_durations_from_local_dir(local_dir, spark_version_filter):
    """Collect test durations from local directories of JUnit XML reports.

    Expects subdirectories matching the pattern test-reports-spark<version>-shard<N>,
    as created by actions/download-artifact.
    """
    all_durations = {}  # suite_name -> duration_minutes
    shard_durations = defaultdict(float)  # shard -> total_duration

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

    # Print per-shard actual durations
    if shard_durations:
        print(f"\n--- Actual per-shard durations (this run) ---")
        for (sv, shard), total in sorted(shard_durations.items()):
            print(f"  Spark {sv}, Shard {shard}: {total:.1f} min")

    return all_durations


def collect_durations_from_run(run_id, spark_version_filter, tmpdir):
    """Collect test durations from a single workflow run."""
    artifacts = list_artifacts(run_id)
    if not artifacts:
        print(f"  No test-report artifacts found in run {run_id}", file=sys.stderr)
        return {}

    if spark_version_filter:
        artifacts = [a for a in artifacts
                     if a["spark_version"] == spark_version_filter]

    run_durations = {}  # suite_name -> duration_minutes
    for art in artifacts:
        label = f"Spark {art['spark_version']}, Shard {art['shard']}"
        print(f"    Downloading {label} (artifact {art['id']})...")
        artifact_dir = download_artifact(art["id"], tmpdir)
        durations, xml_count = parse_artifact_dir(artifact_dir)
        print(f"      -> {xml_count} XML files, {len(durations)} suites")

        for suite_name, dur in durations.items():
            # Average across Spark versions within the same run
            if suite_name in run_durations:
                run_durations[suite_name] = max(run_durations[suite_name], dur)
            else:
                run_durations[suite_name] = dur

    return run_durations


def update_test_parallelization_file(sorted_suites, filepath):
    """Directly update the TOP_N_HIGH_DURATION_TEST_SUITES list in TestParallelization.scala."""
    with open(filepath, 'r') as f:
        content = f.read()

    # Match the full block from the val declaration to the closing paren
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

    new_block = "\n".join(lines)

    # Replace the old block
    start = match.start()
    end = match.end()
    new_content = content[:start] + new_block + content[end:]

    # Also update the reference in the comment above the val
    old_comment_pattern = r'python3 project/scripts/collect_test_durations\.py --pr <N> --top-n \d+ --output-scala'
    new_comment = 'python3 project/scripts/collect_test_durations.py --branch master --update-file'
    new_content = re.sub(old_comment_pattern, new_comment, new_content)

    with open(filepath, 'w') as f:
        f.write(new_content)

    print(f"\nUpdated {filepath} with {n} test suites.")


def print_shard_balance_analysis(sorted_suites, num_shards, num_groups):
    """Simulate the greedy assignment and print shard balance analysis."""
    from collections import defaultdict as dd

    group_durations = [[0.0] * num_groups for _ in range(num_shards)]
    shard_durations = [0.0] * num_shards
    group_counts = [[0] * num_groups for _ in range(num_shards)]

    for name, duration in sorted_suites:
        # Find the group with the smallest duration (greedy assignment)
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
    print(f"{'=' * 80}")
    print(f"Note: Only shows high-duration test assignment. Regular tests are")
    print(f"hash-distributed and add ~0.83 min each on top of these estimates.\n")

    for s in range(num_shards):
        shard_total = shard_durations[s]
        print(f"  Shard {s}: {shard_total:.1f} min total")
        for g in range(num_groups):
            gd = group_durations[s][g]
            gc = group_counts[s][g]
            print(f"    Group {g}: {gd:.1f} min ({gc} suites)")

    max_shard = max(shard_durations)
    min_shard = min(shard_durations)
    print(f"\n  Max shard: {max_shard:.1f} min")
    print(f"  Min shard: {min_shard:.1f} min")
    print(f"  Imbalance: {max_shard - min_shard:.1f} min "
          f"({(max_shard - min_shard) / max_shard * 100:.0f}%)")


def main():
    parser = argparse.ArgumentParser(
        description="Collect test suite durations from CI and rebalance shards"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--run-id", type=int, help="Specific workflow run ID")
    group.add_argument("--branch", type=str,
                       help="Branch name (uses latest successful runs)")
    group.add_argument("--pr", type=int,
                       help="PR number (uses latest successful run)")
    group.add_argument("--local-dir", type=str,
                       help="Local directory containing downloaded test-report "
                            "artifact directories (e.g., test-reports-spark4.0-shard0/)")
    parser.add_argument(
        "--last-n-runs", type=int, default=DEFAULT_LAST_N_RUNS,
        help=f"Number of recent successful runs to average across "
             f"(default: {DEFAULT_LAST_N_RUNS}). Only used with --branch."
    )
    parser.add_argument(
        "--spark-version", type=str, default=None,
        help="Only process a specific Spark version (e.g., '4.0'). "
             "Default: all versions."
    )
    parser.add_argument(
        "--top-n", type=int, default=DEFAULT_TOP_N,
        help=f"Number of slowest suites to include (default: {DEFAULT_TOP_N})."
    )
    parser.add_argument(
        "--output-scala", action="store_true",
        help="Output in Scala format for TestParallelization.scala"
    )
    parser.add_argument(
        "--update-file", action="store_true",
        help="Directly update TestParallelization.scala with the new durations"
    )
    parser.add_argument(
        "--num-shards", type=int, default=4,
        help="Number of shards for balance analysis (default: 4)"
    )
    parser.add_argument(
        "--num-groups", type=int, default=4,
        help="Number of parallel groups per shard for balance analysis (default: 4)"
    )
    args = parser.parse_args()

    # For --pr and --run-id, only use 1 run
    if args.pr or args.run_id:
        args.last_n_runs = 1

    if args.local_dir:
        # Local mode: parse JUnit XML files directly from disk
        print(f"Parsing local test reports from {args.local_dir}...")
        avg_durations = collect_durations_from_local_dir(
            args.local_dir, args.spark_version)
        num_sources = 1
    else:
        # Remote mode: download artifacts from GitHub Actions
        run_ids = get_run_ids(args)
        print(f"\nCollecting durations from {len(run_ids)} run(s)...")
        num_sources = len(run_ids)

        # suite_name -> list of durations across runs
        all_run_durations = defaultdict(list)

        with tempfile.TemporaryDirectory() as tmpdir:
            for i, run_id in enumerate(run_ids):
                print(f"\n--- Run {i + 1}/{len(run_ids)}: {run_id} ---")
                run_durations = collect_durations_from_run(
                    run_id, args.spark_version, tmpdir)

                for suite_name, dur in run_durations.items():
                    all_run_durations[suite_name].append(dur)

        avg_durations = {}
        for suite_name, durs in all_run_durations.items():
            avg_durations[suite_name] = round(sum(durs) / len(durs), 2)

    # Sort by duration descending
    sorted_suites = sorted(avg_durations.items(), key=lambda x: -x[1])

    # Apply top-n filter
    sorted_suites = sorted_suites[:args.top_n]

    # Step 4: Output
    total_suites = len(sorted_suites)
    total_duration = sum(d for _, d in sorted_suites)

    print(f"\n{'=' * 100}")
    print(f"RESULTS: {total_suites} test suites, "
          f"total duration: {total_duration:.1f} minutes")
    print(f"Averaged across {num_sources} run(s)")
    print(f"{'=' * 100}\n")

    if args.output_scala or args.update_file:
        n = len(sorted_suites)
        print(f"  /** {n} slowest test suites and their durations (in minutes). */")
        print(f"  val TOP_{n}_HIGH_DURATION_TEST_SUITES: "
              f"List[(String, Double)] = List(")
        for i, (name, dur) in enumerate(sorted_suites):
            comma = "," if i < len(sorted_suites) - 1 else ""
            print(f'    ("{name}", {dur}){comma}')
        print("  )")
    else:
        print(f"{'#':<5} {'Suite':<90} {'Avg (min)':>10} {'Runs':>5}")
        print("-" * 112)
        for i, (name, dur) in enumerate(sorted_suites, 1):
            runs = len(all_run_durations[name]) if not args.local_dir else 1
            print(f"{i:<5} {name:<90} {dur:>8.2f}m {runs:>5}")

    # Summary stats
    if sorted_suites:
        top10 = sum(d for _, d in sorted_suites[:10])
        top50 = sum(d for _, d in sorted_suites[:50])
        print(f"\n--- Summary ---")
        print(f"Total suites: {total_suites}")
        print(f"Total duration (sum): {total_duration:.1f} min")
        print(f"Top 10 account for: {top10:.1f} min "
              f"({top10 / total_duration * 100:.0f}% of total)")
        if len(sorted_suites) >= 50:
            print(f"Top 50 account for: {top50:.1f} min "
                  f"({top50 / total_duration * 100:.0f}% of total)")
        print(f"Median duration: "
              f"{sorted_suites[len(sorted_suites)//2][1]:.2f} min")
        print(f"Mean duration: {total_duration / total_suites:.2f} min")

    # Shard balance analysis
    print_shard_balance_analysis(sorted_suites, args.num_shards, args.num_groups)

    # Step 5: Optionally update the file
    if args.update_file:
        update_test_parallelization_file(sorted_suites, TEST_PARALLELIZATION_FILE)
        print("\nDone! Review the changes with: git diff project/TestParallelization.scala")


if __name__ == "__main__":
    main()
