#!/usr/bin/env python3
"""
Cross-Spark Version Build Testing

Tests the Delta Lake build system by validating JAR file names for:
1. Default publish (publishM2) - should publish ALL modules
2. Spark-specific publish (runOnlyForReleasableSparkModules) - should publish only Spark-dependent modules

Usage:
    python project/tests/test_cross_spark_publish.py

The script will:
1. Test default publishM2 command publishes all modules for default Spark version
2. Test runOnlyForReleasableSparkModules command publishes only Spark-dependent modules
3. Test full cross-version build workflow
4. Exit with status 0 on success, 1 on failure
"""

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Set, Dict


# Spark-related modules (requiresCrossSparkBuild := true)
# These modules get a Spark version suffix (e.g., _4.0) for non-default versions
# Template format: {suffix} = short Spark version suffix (e.g., "", "_4.0")
#                  {version} = full Delta version (e.g., "3.4.0-SNAPSHOT")
SPARK_RELATED_JAR_TEMPLATES = [
    "delta-spark{suffix}_2.13-{version}.jar",
    "delta-connect-common{suffix}_2.13-{version}.jar",
    "delta-connect-client{suffix}_2.13-{version}.jar",
    "delta-connect-server{suffix}_2.13-{version}.jar",
    "delta-sharing-spark{suffix}_2.13-{version}.jar",
    "delta-contribs{suffix}_2.13-{version}.jar",
    "delta-iceberg{suffix}_2.13-{version}.jar"
]

# Non-spark-related modules (built once, same for all Spark versions)
# Template format: {version} = Delta version (e.g., "3.4.0-SNAPSHOT")
NON_SPARK_RELATED_JAR_TEMPLATES = [
    # Scala modules
    "delta-hudi_2.13-{version}.jar",
    "delta-standalone_2.13-{version}.jar",

    # Java-only modules (no Scala version)
    "delta-storage-{version}.jar",
    "delta-kernel-api-{version}.jar",
    "delta-kernel-defaults-{version}.jar",
    "delta-storage-s3-dynamodb-{version}.jar",
    "delta-unity-{version}.jar"
]


@dataclass
class SparkVersionSpec:
    """Configuration for a specific Spark version."""
    suffix: str  # e.g., "" for default, "_X.Y" for other versions

    def __post_init__(self):
        """Generate JAR templates with the suffix applied."""
        # Generate Spark-related JAR templates with the suffix
        self.spark_related_jars = [
            jar.format(suffix=self.suffix, version="{version}")
            for jar in SPARK_RELATED_JAR_TEMPLATES
        ]

        # Non-Spark-related JAR templates are the same for all Spark versions
        self.non_spark_related_jars = list(NON_SPARK_RELATED_JAR_TEMPLATES)

    @property
    def all_jars(self) -> List[str]:
        """All JAR templates for this Spark version (Spark-related + non-Spark-related)."""
        return self.spark_related_jars + self.non_spark_related_jars


# Spark versions to test (key = full version string, value = spec with suffix)
SPARK_VERSIONS: Dict[str, SparkVersionSpec] = {
    "3.5.7": SparkVersionSpec(""),      # Default Spark version without suffix
    "4.0.2-SNAPSHOT": SparkVersionSpec("_4.0") # Other Spark versions with suffix
}

# The default Spark version (no suffix in artifact names)
DEFAULT_SPARK = "3.5.7"


def substitute_xversion(jar_templates: List[str], delta_version: str) -> Set[str]:
    """
    Substitutes {version} placeholder in JAR templates with actual Delta version.
    """
    return {jar.format(version=delta_version) for jar in jar_templates}


class CrossSparkPublishTest:
    """Tests cross-Spark version builds."""

    def __init__(self, delta_root: Path):
        self.delta_root = delta_root
        self.delta_version = self._get_delta_version()
        self.scala_version = "2.13"

    def _get_delta_version(self) -> str:
        """Reads Delta version from version.sbt."""
        with open(self.delta_root / "version.sbt", 'r') as f:
            for line in f:
                if 'version :=' in line:
                    return line.split('"')[1]
        sys.exit("Error: Could not parse version from version.sbt")

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

    def find_all_jars(self) -> Set[str]:
        """Finds all JAR files from Maven local repository."""
        m2_repo = Path.home() / ".m2" / "repository" / "io" / "delta"

        if not m2_repo.exists():
            return set()

        found_jars = set()
        for version_dir in m2_repo.rglob(self.delta_version):
            for jar_file in version_dir.glob("*.jar"):
                # Exclude test/source/javadoc JARs
                if not any(x in jar_file.name for x in ["-tests", "-sources", "-javadoc"]):
                    found_jars.add(jar_file.name)

        return found_jars

    def run_sbt_command(self, description: str, command: List[str]) -> bool:
        """Runs an SBT command and returns True if successful."""
        print(f"  {description}")
        try:
            subprocess.run(command, cwd=self.delta_root, check=True,
                          stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
            return True
        except subprocess.CalledProcessError:
            print(f"  ✗ Command failed: {' '.join(command)}")
            return False

    def validate_jars(self, expected: Set[str], test_name: str) -> bool:
        """Validates that found JARs match expected JARs exactly."""
        found = self.find_all_jars()

        print(f"\n{test_name} - Found JARs ({len(found)} total):")
        for jar in sorted(found):
            print(f"  {jar}")

        print(f"\n{test_name} - Expected JARs ({len(expected)} total):")
        for jar in sorted(expected):
            print(f"  {jar}")

        missing = expected - found
        extra = found - expected

        print()
        if not missing and not extra:
            print(f"✓ {test_name} - All expected JARs found")
            return True

        if missing:
            print(f"✗ {test_name} - Missing JARs ({len(missing)}):")
            for jar in sorted(missing):
                print(f"  ✗ {jar}")

        if extra:
            print(f"\n✗ {test_name} - Unexpected JARs ({len(extra)}):")
            for jar in sorted(extra):
                print(f"  ✗ {jar}")

        return False

    def test_default_publish(self) -> bool:
        """Default publishM2 should publish ALL modules for default Spark version."""
        spark_spec = SPARK_VERSIONS[DEFAULT_SPARK]

        print("\n" + "="*70)
        print(f"TEST: Default publishM2 (should publish ALL modules for Spark {DEFAULT_SPARK})")
        print("="*70)

        self.clean_maven_cache()

        if not self.run_sbt_command(
            "Running: build/sbt publishM2",
            ["build/sbt", "publishM2"]
        ):
            return False

        expected = substitute_xversion(spark_spec.all_jars, self.delta_version)
        return self.validate_jars(expected, "Default publishM2")

    def test_run_only_for_spark_modules(self) -> bool:
        """runOnlyForReleasableSparkModules should publish only Spark-dependent modules."""
        spark_version = "4.0.2-SNAPSHOT"
        spark_spec = SPARK_VERSIONS[spark_version]

        print("\n" + "="*70)
        print(f"TEST: runOnlyForReleasableSparkModules (should publish only Spark-dependent modules for Spark {spark_version})")
        print("="*70)

        self.clean_maven_cache()

        if not self.run_sbt_command(
            f"Running: build/sbt -DsparkVersion={spark_version} \"runOnlyForReleasableSparkModules publishM2\"",
            ["build/sbt", f"-DsparkVersion={spark_version}", "runOnlyForReleasableSparkModules publishM2"]
        ):
            return False

        expected = substitute_xversion(spark_spec.spark_related_jars, self.delta_version)
        return self.validate_jars(expected, "runOnlyForReleasableSparkModules")

    def test_cross_spark_workflow(self) -> bool:
        """Full cross-Spark workflow (publishM2 + runOnlyForReleasableSparkModules)."""
        default_spec = SPARK_VERSIONS[DEFAULT_SPARK]

        print("\n" + "="*70)
        print("TEST: Cross-Spark Workflow (all Spark versions)")
        print("="*70)

        self.clean_maven_cache()

        # Step 1: Publish all modules for default Spark version
        if not self.run_sbt_command(
            f"Step 1: build/sbt publishM2 (Spark {DEFAULT_SPARK} - all modules)",
            ["build/sbt", "publishM2"]
        ):
            return False

        # Step 2: Publish only Spark-dependent modules for other Spark versions
        for spark_version, spark_spec in SPARK_VERSIONS.items():
            if spark_version == DEFAULT_SPARK:
                continue  # Skip default, already published

            if not self.run_sbt_command(
                f"Step 2: build/sbt -DsparkVersion={spark_version} \"runOnlyForReleasableSparkModules publishM2\" (Spark {spark_version} - Spark-dependent only)",
                ["build/sbt", f"-DsparkVersion={spark_version}", "runOnlyForReleasableSparkModules publishM2"]
            ):
                return False

        # Build expected JARs: Spark-related for all versions + non-Spark-related once
        expected = set()
        for spark_spec in SPARK_VERSIONS.values():
            expected.update(substitute_xversion(spark_spec.spark_related_jars, self.delta_version))
        expected.update(substitute_xversion(SPARK_VERSIONS[DEFAULT_SPARK].non_spark_related_jars, self.delta_version))

        return self.validate_jars(expected, "Cross-Spark Workflow")

    def validate_spark_versions(self) -> None:
        """
        Validates that Spark versions in this test match those in CrossSparkVersions.scala.

        Uses 'build/sbt showSparkVersions' to query versions directly from the build.
        """
        try:
            # Query Spark versions from SBT
            result = subprocess.run(
                ["build/sbt", "showSparkVersions"],
                cwd=self.delta_root,
                capture_output=True,
                text=True,
                check=True
            )

            # Parse output - each line is a Spark version
            # Version format: X.Y.Z or X.Y.Z-SNAPSHOT
            import re
            version_pattern = re.compile(r'^\d+\.\d+\.\d+(-SNAPSHOT)?$')

            build_versions = set()
            for line in result.stdout.strip().split('\n'):
                line = line.strip()
                if version_pattern.match(line):
                    build_versions.add(line)

            # Get Python test versions
            test_versions = set(SPARK_VERSIONS.keys())

            # Compare versions
            if build_versions != test_versions:
                missing_in_test = build_versions - test_versions
                extra_in_test = test_versions - build_versions

                print("\n" + "="*70)
                print("ERROR: Spark version mismatch between test and build")
                print("="*70)

                if missing_in_test:
                    print(f"\n✗ Build defines these versions, missing in test:")
                    for v in sorted(missing_in_test):
                        print(f"    {v}")

                if extra_in_test:
                    print(f"\n✗ Test defines these versions, missing in build:")
                    for v in sorted(extra_in_test):
                        print(f"    {v}")

                print("\nPlease update SPARK_VERSIONS in this test to match build configuration.")
                print("="*70 + "\n")
                sys.exit(1)

            # Success - silent validation
            print(f"✓ Spark versions: {', '.join(sorted(build_versions))}\n")

        except subprocess.CalledProcessError as e:
            print(f"Warning: Could not validate Spark versions: {e}\n")


def main():
    """Main entry point."""
    try:
        delta_root = Path(__file__).parent.parent.parent
        if not (delta_root / "build.sbt").exists():
            print("Error: build.sbt not found. Run from Delta repository root.")
            sys.exit(1)

        print("="*70)
        print("Cross-Spark Build Test Suite")
        print("="*70)
        print()

        # Create test object and validate Spark versions
        test = CrossSparkPublishTest(delta_root)
        test.validate_spark_versions()

        # Run all tests
        test1_passed = test.test_default_publish()
        test2_passed = test.test_run_only_for_spark_modules()
        test3_passed = test.test_cross_spark_workflow()

        # Summary
        print("\n" + "="*70)
        print("TEST SUMMARY")
        print("="*70)
        print(f"Default publishM2:                      {'✓ PASSED' if test1_passed else '✗ FAILED'}")
        print(f"runOnlyForReleasableSparkModules:       {'✓ PASSED' if test2_passed else '✗ FAILED'}")
        print(f"Cross-Spark Workflow:                   {'✓ PASSED' if test3_passed else '✗ FAILED'}")
        print("="*70)

        if test1_passed and test2_passed and test3_passed:
            print("\n✓ ALL TESTS PASSED")
            sys.exit(0)
        else:
            print("\n✗ SOME TESTS FAILED")
            sys.exit(1)
    except Exception as e:
        print(f"\n✗ TEST EXECUTION FAILED WITH ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
