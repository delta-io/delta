#!/usr/bin/env python3
"""
Resolve source-built Spark metadata for CI workflows.

`sparkVersion` selects Delta's compatibility line (shims, JVM settings, artifact
suffixes). `spark-source-ref` optionally overrides the source ref to build. When
no override is provided, the default ref comes from CrossSparkVersions.scala via
target/spark-versions.json.
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def run_command(args, cwd):
    result = subprocess.run(
        args,
        cwd=str(cwd),
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    return result.stdout.strip()


def get_spark_field(repo_root, spark_version, field):
    script = repo_root / "project" / "scripts" / "get_spark_version_info.py"
    output = run_command(
        [sys.executable, str(script), "--get-field", spark_version, field],
        repo_root,
    )
    return json.loads(output)


def resolve_spark_sha(spark_repo, spark_ref, spark_dir):
    spark_dir.mkdir(parents=True, exist_ok=True)
    if not (spark_dir / ".git").exists():
        run_command(["git", "-C", str(spark_dir), "init"], spark_dir.parent)

    try:
        run_command(["git", "-C", str(spark_dir), "remote", "get-url", "origin"], spark_dir.parent)
        run_command(
            ["git", "-C", str(spark_dir), "remote", "set-url", "origin", spark_repo],
            spark_dir.parent,
        )
    except subprocess.CalledProcessError:
        run_command(
            ["git", "-C", str(spark_dir), "remote", "add", "origin", spark_repo],
            spark_dir.parent,
        )

    run_command(["git", "-C", str(spark_dir), "fetch", "--depth", "1", "origin", spark_ref], spark_dir.parent)
    return run_command(["git", "-C", str(spark_dir), "rev-parse", "FETCH_HEAD"], spark_dir.parent)


def emit_output(values):
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as handle:
            for key, value in values.items():
                handle.write("{}={}\n".format(key, value))

    for key, value in values.items():
        print("{}={}".format(key, value))


def strip_suffix(value, suffix):
    if value.endswith(suffix):
        return value[: -len(suffix)]
    return value


def main():
    parser = argparse.ArgumentParser(description="Resolve source-built Spark metadata")
    parser.add_argument("--spark-version", required=True, help="Spark compatibility line")
    parser.add_argument(
        "--spark-source-ref",
        default="",
        help="Optional Spark source ref override; blank uses version metadata",
    )
    parser.add_argument(
        "--spark-repo",
        default="https://github.com/apache/spark.git",
        help="Spark git repository",
    )
    parser.add_argument(
        "--spark-dir",
        default="/tmp/spark",
        help="Temporary Spark checkout used for ref resolution",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    full_version = get_spark_field(repo_root, args.spark_version, "fullVersion")
    short_version = get_spark_field(repo_root, args.spark_version, "shortVersion")
    is_master = get_spark_field(repo_root, args.spark_version, "isMaster")
    default_ref = get_spark_field(repo_root, args.spark_version, "sourceBuildDefaultRef")
    artifact_base_version = get_spark_field(
        repo_root, args.spark_version, "sourceBuildArtifactBaseVersion"
    )

    source_ref = args.spark_source_ref.strip() or (default_ref or "")
    if not source_ref:
        raise SystemExit(
            "No Spark source ref configured for Spark {}. Provide --spark-source-ref "
            "or sourceBuildDefaultRef in CrossSparkVersions.scala.".format(args.spark_version)
        )

    if not artifact_base_version:
        artifact_base_version = strip_suffix(full_version, "-SNAPSHOT")

    normalized_spark_version = "master" if is_master else short_version
    spark_sha = resolve_spark_sha(args.spark_repo, source_ref, Path(args.spark_dir))
    spark_short_sha = spark_sha[:12]
    spark_artifact_version = "{}-{}-SNAPSHOT".format(artifact_base_version, spark_short_sha)

    print(
        "Resolved Spark {} source ref {} to {} and Maven version {}".format(
            normalized_spark_version, source_ref, spark_sha, spark_artifact_version
        )
    )
    emit_output(
        {
            "spark_version": normalized_spark_version,
            "source_ref": source_ref,
            "spark_sha": spark_sha,
            "artifact_base_version": artifact_base_version,
            "spark_artifact_version": spark_artifact_version,
        }
    )


if __name__ == "__main__":
    main()
