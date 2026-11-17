#!/usr/bin/env python3
"""Select Master CI workflows from changes since the last successful run."""

import json
import os
import subprocess
import sys
import urllib.parse
import urllib.request


WORKFLOWS = (
    "build",
    "changelog_tests_spark42",
    "flink_test",
    "iceberg_test",
    "kernel_test",
    "kernel_unitycatalog_test",
    "spark_examples_test",
    "spark_python_test",
    "spark_test",
    "unidoc",
)


def selections(paths):
    """Return the workflows selected by the former push path filters."""
    paths = set(paths)
    if any(
        path == ".github/workflows/master_ci.yaml"
        or path == ".github/scripts/master_ci_changes.py"
        for path in paths
    ):
        return {name: True for name in WORKFLOWS}

    non_docs = any(not path.endswith((".md", ".txt")) for path in paths)
    selected = {name: non_docs for name in WORKFLOWS if name != "unidoc"}
    selected["unidoc"] = bool(paths)
    selected["flink_test"] = any(
        (path.startswith(("flink/", "kernel/"))
         or path in {
             "build.sbt",
             "project/CrossFlinkVersions.scala",
             ".github/workflows/flink_test.yaml",
         })
        and not path.endswith((".md", ".txt"))
        for path in paths
    )
    selected["kernel_unitycatalog_test"] = any(
        path in {
            "build.sbt",
            "version.sbt",
            ".github/workflows/kernel_unitycatalog_test.yaml",
        }
        or (
            path.startswith(("kernel/", "storage/"))
            and path.endswith((".scala", ".java"))
        )
        for path in paths
    )
    return selected


def latest_successful_head(repository, ref_name, token):
    query = urllib.parse.urlencode(
        {"branch": ref_name, "event": "push", "status": "success", "per_page": 100}
    )
    url = f"https://api.github.com/repos/{repository}/actions/runs?{query}"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        runs = json.load(response)["workflow_runs"]
    matching_runs = [run for run in runs if run["name"] == "Master CI"]
    return matching_runs[0]["head_sha"] if matching_runs else None


def changed_paths(base, head):
    output = subprocess.check_output(
        ["git", "diff", "--name-only", "--diff-filter=ACDMRTUXB", base, head],
        text=True,
        stderr=subprocess.STDOUT,
    )
    return [path for path in output.splitlines() if path]


def write_outputs(selected, base=""):
    output_path = os.environ.get("GITHUB_OUTPUT")
    stream = open(output_path, "a") if output_path else sys.stdout
    try:
        for name in WORKFLOWS:
            print(f"{name}={str(selected[name]).lower()}", file=stream)
        print(f"base_sha={base}", file=stream)
    finally:
        if output_path:
            stream.close()


def main():
    all_workflows = {name: True for name in WORKFLOWS}
    if os.environ.get("GITHUB_EVENT_NAME") != "push":
        write_outputs(all_workflows)
        return

    try:
        base = latest_successful_head(
            os.environ["GITHUB_REPOSITORY"],
            os.environ["GITHUB_REF_NAME"],
            os.environ["GITHUB_TOKEN"],
        )
        if not base:
            print("No successful Master CI run found; running every workflow.")
            write_outputs(all_workflows)
            return
        paths = changed_paths(base, os.environ["GITHUB_SHA"])
        print(f"Selecting from {len(paths)} path(s) changed since {base}:")
        print("\n".join(paths))
        write_outputs(selections(paths), base)
    except Exception as error:
        print(f"Could not determine changes ({error}); running every workflow.", file=sys.stderr)
        write_outputs(all_workflows)


if __name__ == "__main__":
    main()
