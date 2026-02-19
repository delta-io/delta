#!/usr/bin/env python3
"""
Collect per-suite test durations from JUnit XML test reports uploaded as
GitHub Actions artifacts.

Downloads test-report artifacts from a Delta Spark CI run and parses the
JUnit XML files to extract exact per-suite durations. Outputs a sorted list
suitable for updating the TOP_N_HIGH_DURATION_TEST_SUITES list in
project/TestParallelization.scala.

Usage:
    # Use the latest successful run on a branch:
    python3 project/scripts/collect_test_durations.py --branch master

    # Use a specific workflow run ID:
    python3 project/scripts/collect_test_durations.py --run-id 22150233443

    # Use a specific PR:
    python3 project/scripts/collect_test_durations.py --pr 6069

    # Output in Scala format for copy-pasting into TestParallelization.scala:
    python3 project/scripts/collect_test_durations.py --branch master --output-scala

    # Only show top 100 slowest suites:
    python3 project/scripts/collect_test_durations.py --branch master --top-n 100

Prerequisites:
    Before running this script, you must temporarily enable JUnit XML test reports
    and artifact uploads in the CI workflow:

    1. Add to build.sbt (in the commonSettings block, after the testOptions line):
         Test / testOptions += Tests.Argument("-u", "target/test-reports"),

    2. Add to .github/workflows/spark_test.yaml (after the "Run Scala/Java tests" step):
         - name: Upload test reports
           if: always()
           uses: actions/upload-artifact@v4
           with:
             name: test-reports-spark${{ matrix.spark_version }}-shard${{ matrix.shard }}
             path: "**/target/test-reports/*.xml"
             retention-days: 7

    3. Push, wait for CI to complete, then run this script.
    4. Revert the build.sbt and workflow changes after collecting durations.

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


def get_run_id(args):
    """Resolve the workflow run ID from CLI arguments."""
    if args.run_id:
        return args.run_id

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

    # Find the latest "Delta Spark" workflow run on this branch
    runs_json = run_gh([
        "run", "list", "--repo", REPO,
        "--branch", branch,
        "--workflow", "spark_test.yaml",
        "--status", "success",
        "--limit", "1",
        "--json", "databaseId,headSha,createdAt"
    ])
    runs = json.loads(runs_json)
    if not runs:
        print(f"No successful Delta Spark runs found on branch '{branch}'",
              file=sys.stderr)
        sys.exit(1)

    run_id = runs[0]["databaseId"]
    print(f"Found run {run_id} (commit: {runs[0]['headSha'][:8]}, "
          f"created: {runs[0]['createdAt']})")
    return run_id


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
    Parse a JUnit XML file and return (suite_name, duration_seconds).

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


def main():
    parser = argparse.ArgumentParser(
        description="Collect test suite durations from JUnit XML test reports"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--run-id", type=int, help="Specific workflow run ID")
    group.add_argument("--branch", type=str,
                       help="Branch name (uses latest successful run)")
    group.add_argument("--pr", type=int,
                       help="PR number (uses latest successful run)")
    parser.add_argument(
        "--spark-version", type=str, default=None,
        help="Only process a specific Spark version (e.g., '4.0'). "
             "Default: all versions."
    )
    parser.add_argument(
        "--top-n", type=int, default=None,
        help="Only show top N slowest suites. Default: show all."
    )
    parser.add_argument(
        "--output-scala", action="store_true",
        help="Output in Scala format for TestParallelization.scala"
    )
    args = parser.parse_args()

    # Step 1: Resolve run ID
    run_id = get_run_id(args)

    # Step 2: List test-report artifacts
    print("\nFetching artifacts...")
    artifacts = list_artifacts(run_id)
    if not artifacts:
        print("No test-report artifacts found in this run!", file=sys.stderr)
        print("Make sure the workflow uploads test-reports artifacts.",
              file=sys.stderr)
        print("See: .github/workflows/spark_test.yaml", file=sys.stderr)
        sys.exit(1)

    if args.spark_version:
        artifacts = [a for a in artifacts
                     if a["spark_version"] == args.spark_version]

    spark_versions = sorted(set(a["spark_version"] for a in artifacts))
    num_shards = max(a["shard"] for a in artifacts) + 1
    print(f"Found {len(artifacts)} test-report artifacts "
          f"(Spark versions: {spark_versions}, {num_shards} shards)")

    # Step 3: Download, extract, and parse all artifacts
    all_durations = defaultdict(list)  # suite_name -> [durations]

    with tempfile.TemporaryDirectory() as tmpdir:
        for art in artifacts:
            label = f"Spark {art['spark_version']}, Shard {art['shard']}"
            print(f"  Downloading {label} (artifact {art['id']})...")
            artifact_dir = download_artifact(art["id"], tmpdir)
            durations, xml_count = parse_artifact_dir(artifact_dir)
            print(f"    -> {xml_count} XML files, {len(durations)} suites")

            for suite_name, dur in durations.items():
                all_durations[suite_name].append(dur)

    # Step 4: Compute average duration across Spark versions
    avg_durations = {}
    for suite_name, durs in all_durations.items():
        avg_durations[suite_name] = round(sum(durs) / len(durs), 2)

    # Sort by duration descending
    sorted_suites = sorted(avg_durations.items(), key=lambda x: -x[1])

    if args.top_n:
        sorted_suites = sorted_suites[:args.top_n]

    # Step 5: Output
    total_suites = len(sorted_suites)
    total_duration = sum(d for _, d in sorted_suites)

    print(f"\n{'=' * 100}")
    print(f"RESULTS: {total_suites} test suites, "
          f"total duration: {total_duration:.1f} minutes")
    print(f"Averaged across Spark versions: {spark_versions}")
    print(f"{'=' * 100}\n")

    if args.output_scala:
        n = len(sorted_suites)
        print(f"  /** {n} slowest test suites and their durations. */")
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
            runs = len(all_durations[name])
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
        print(f"Top 50 account for: {top50:.1f} min "
              f"({top50 / total_duration * 100:.0f}% of total)")
        print(f"Median duration: "
              f"{sorted_suites[len(sorted_suites)//2][1]:.2f} min")
        print(f"Mean duration: {total_duration / total_suites:.2f} min")


if __name__ == "__main__":
    main()
