#!/usr/bin/env python3
"""
Cross-Spark Version Build Testing

Tests the Delta Lake build system by publishing and validating JAR file names for
multiple Spark versions (3.5.7 and 4.0.2-SNAPSHOT).

Usage:
    python project/tests/test_cross_spark_publish.py

The script will:
1. Clean Maven local cache (~/.m2/repository/io/delta/)
2. Publish JARs to Maven local using 'build/sbt "crossSparkRelease publishM2"'
3. Validate that all expected JAR files exist in ~/.m2/repository with correct names:
   - Spark-dependent modules: published for both Spark 3.5.7 and 4.0
   - Spark-independent modules: published once
4. Exit with status 0 on success, 1 on failure
"""

import subprocess
import sys
from pathlib import Path
from typing import List


def get_expected_jars(delta_version: str, scala_version: str = "2.13") -> List[str]:
    """Returns all expected JAR names for all Spark versions."""
    jars = []

    # Spark-dependent modules (requiresCrossSparkBuild := true)
    # These are built for both Spark 3.5.7 (no suffix) and Spark 4.0 (_4.0 suffix)
    spark_dependent = ["delta-spark", "delta-connect-common", "delta-connect-client",
                       "delta-connect-server", "delta-sharing-spark", "delta-contribs", "delta-iceberg"]

    # Spark 3.5.7 (latest - no Spark version suffix)
    for module in spark_dependent:
        jars.append(f"{module}_{scala_version}-{delta_version}.jar")

    # Spark 4.0 (with _4.0 suffix)
    for module in spark_dependent:
        jars.append(f"{module}_4.0_{scala_version}-{delta_version}.jar")

    # Spark-independent modules with Scala version (built once)
    for module in ["delta-hudi", "delta-standalone"]:
        jars.append(f"{module}_{scala_version}-{delta_version}.jar")

    # Spark-independent Java-only modules (built once, no Scala version)
    for module in ["delta-storage", "delta-kernel-api", "delta-kernel-defaults",
                   "delta-storage-s3-dynamodb", "delta-unity"]:
        jars.append(f"{module}-{delta_version}.jar")

    return jars


class CrossSparkPublishTest:
    """Tests cross-Spark version builds."""

    def __init__(self, delta_root: Path, version: str):
        self.delta_root = delta_root
        self.delta_version = version
        self.scala_version = "2.13"

    def clean_maven_cache(self) -> None:
        """Clears Maven local cache for io.delta artifacts."""
        import shutil

        m2_repo = Path.home() / ".m2" / "repository" / "io" / "delta"

        if m2_repo.exists():
            print(f"Cleaning Maven cache: {m2_repo}")
            shutil.rmtree(m2_repo)
            print("✓ Maven cache cleaned\n")
        else:
            print("Maven cache already clean\n")

    def find_all_jars(self) -> List[str]:
        """Finds all JAR files from Maven local repository."""
        # Maven local repository location
        m2_repo = Path.home() / ".m2" / "repository" / "io" / "delta"

        if not m2_repo.exists():
            return []

        found_jars = set()
        # Search for all JARs in io.delta Maven artifacts
        for version_dir in m2_repo.rglob(self.delta_version):
            for jar_file in version_dir.glob("*.jar"):
                # Exclude test/source/javadoc JARs
                if not any(x in jar_file.name for x in ["-tests", "-sources", "-javadoc"]):
                    found_jars.add(jar_file.name)

        return sorted(found_jars)

    def build_all_versions(self) -> bool:
        """Builds and publishes JARs for all Spark versions."""
        print("Publishing JARs with: build/sbt \"crossSparkRelease publishM2\"\n")
        try:
            subprocess.run(["build/sbt", "crossSparkRelease publishM2"],
                         cwd=self.delta_root, check=True)
            return True
        except subprocess.CalledProcessError:
            print("✗ Build failed")
            return False

    def validate_all_versions(self) -> bool:
        """Validates that found JARs match expected JARs exactly."""
        expected = set(get_expected_jars(self.delta_version, self.scala_version))
        found = set(self.find_all_jars())

        print(f"\nFound JARs in Maven repository ({len(found)} total):")
        for jar in sorted(found):
            print(f"  {jar}")

        print(f"\nExpected JARs ({len(expected)} total):")
        for jar in sorted(expected):
            print(f"  {jar}")

        missing = expected - found
        extra = found - expected

        print()  # blank line
        if not missing and not extra:
            print("✓ All expected JARs found - validation passed")
            return True

        if missing:
            print(f"✗ Missing JARs ({len(missing)}):")
            for jar in sorted(missing):
                print(f"  ✗ {jar}")

        if extra:
            print(f"\n✗ Unexpected JARs ({len(extra)}):")
            for jar in sorted(extra):
                print(f"  ✗ {jar}")

        return False


def get_delta_version(delta_root: Path) -> str:
    """Reads Delta version from version.sbt."""
    with open(delta_root / "version.sbt", 'r') as f:
        for line in f:
            if 'version :=' in line:
                return line.split('"')[1]
    sys.exit("Error: Could not parse version from version.sbt")


def main():
    """Main entry point."""
    delta_root = Path(__file__).parent.parent.parent
    if not (delta_root / "build.sbt").exists():
        sys.exit("Error: build.sbt not found. Run from Delta repository root.")

    print("Cross-Spark Build Test\n")

    test = CrossSparkPublishTest(delta_root, get_delta_version(delta_root))

    # Clean Maven cache before publishing
    test.clean_maven_cache()

    if not test.build_all_versions():
        sys.exit(1)

    if test.validate_all_versions():
        print("\n✓ ALL TESTS PASSED")
        sys.exit(0)
    else:
        print("\n✗ TESTS FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
