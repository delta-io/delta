#!/usr/bin/env python3
"""
Cross-Spark Version Build Testing

Tests the Delta Lake build system by validating JAR file names for:
1. Default publish (publishM2) - publishes ALL modules WITH Spark suffix
2. Backward-compat publish (skipSparkSuffix=true) - publishes WITHOUT suffix
3. Full cross-version workflow publishes both with and without suffix

Usage:
    python project/tests/test_cross_spark_publish.py

The script will:
1. Test default publishM2 command publishes all modules WITH Spark suffix
2. Test skipSparkSuffix=true publishes WITHOUT suffix (backward compatibility)
3. Test full cross-version build workflow (both with and without suffix)
4. Exit with status 0 on success, 1 on failure
"""

import json
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
]

# Iceberg-related modules - only built for Spark versions with supportIceberg=true
# delta-iceberg has no Spark suffix (always delta-iceberg_2.13) because it only supports Spark 4.0
DELTA_ICEBERG_JAR_TEMPLATES = [
    "delta-iceberg_2.13-{version}.jar",
]

# Hudi-related modules - only built for Spark versions with supportHudi=true
# delta-hudi has no Spark suffix (always delta-hudi_2.13)
DELTA_HUDI_JAR_TEMPLATES = [
    "delta-hudi_2.13-{version}.jar",
]

# Non-spark-related modules (built once, same for all Spark versions)
# Template format: {version} = Delta version (e.g., "3.4.0-SNAPSHOT")
NON_SPARK_RELATED_JAR_TEMPLATES = [
    "delta-storage-{version}.jar",
    "delta-kernel-api-{version}.jar",
    "delta-kernel-defaults-{version}.jar",
    "delta-storage-s3-dynamodb-{version}.jar",
    "delta-kernel-unitycatalog-{version}.jar",
    "delta-contribs_2.13-{version}.jar",
]


@dataclass
class SparkVersionSpec:
    """Configuration for a specific Spark version.

    Mirrors the SparkVersionSpec in CrossSparkVersions.scala.
    """
    suffix: str  # e.g., "" for default, "_X.Y" for other versions
    support_iceberg: bool = False  # Whether this Spark version supports iceberg integration
    support_hudi: bool = True  # Whether this Spark version supports hudi integration

    def __post_init__(self):
        """Generate JAR templates with the suffix applied."""
        # Generate Spark-related JAR templates with the suffix
        self.spark_related_jars = [
            jar.format(suffix=self.suffix, version="{version}")
            for jar in SPARK_RELATED_JAR_TEMPLATES
        ]

        # Iceberg JARs have no Spark suffix (always delta-iceberg_2.13)
        if self.support_iceberg:
            self.iceberg_jars = list(DELTA_ICEBERG_JAR_TEMPLATES)
        else:
            self.iceberg_jars = []

        # Hudi JARs have no Spark suffix (always delta-hudi_2.13)
        if self.support_hudi:
            self.hudi_jars = list(DELTA_HUDI_JAR_TEMPLATES)
        else:
            self.hudi_jars = []

        # Non-Spark-related JAR templates are the same for all Spark versions
        self.non_spark_related_jars = list(NON_SPARK_RELATED_JAR_TEMPLATES)

    @property
    def all_jars(self) -> List[str]:
        """All JAR templates for this Spark version."""
        return self.spark_related_jars + self.non_spark_related_jars + self.iceberg_jars + self.hudi_jars


# Spark versions to test (key = full version string, value = spec with suffix)
# By default, ALL versions get a Spark suffix (e.g., delta-spark_4.0_2.13)
# skipSparkSuffix=true removes the suffix (used during release for backward compat)
# These should mirror CrossSparkVersions.scala
SPARK_VERSIONS: Dict[str, SparkVersionSpec] = {
    "4.0.1": SparkVersionSpec(suffix="_4.0", support_iceberg=True, support_hudi=True),
    "4.1.0": SparkVersionSpec(suffix="_4.1", support_iceberg=False, support_hudi=False)
}

# The default Spark version
# This is intentionally hardcoded here to explicitly test the default version.
DEFAULT_SPARK = "4.1.0"


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
        """Default publishM2 should publish ALL modules WITH Spark suffix."""
        spark_spec = SPARK_VERSIONS[DEFAULT_SPARK]

        print("\n" + "="*70)
        print(f"TEST: Default publishM2 (should publish ALL modules WITH suffix for Spark {DEFAULT_SPARK})")
        print("="*70)

        self.clean_maven_cache()

        if not self.run_sbt_command(
            "Running: build/sbt publishM2",
            ["build/sbt", "publishM2"]
        ):
            return False

        # Default behavior: all Spark-dependent modules have suffix (e.g., delta-spark_4.0_2.13)
        expected = substitute_xversion(spark_spec.all_jars, self.delta_version)
        return self.validate_jars(expected, "Default publishM2 (with suffix)")

    def test_backward_compat_publish(self) -> bool:
        """skipSparkSuffix=true should publish ALL modules WITHOUT Spark suffix."""
        # Create a spec without suffix for backward compatibility
        # Uses the same iceberg support as the default Spark version
        default_spark_spec = SPARK_VERSIONS[DEFAULT_SPARK]
        spark_spec_no_suffix = SparkVersionSpec(suffix="", support_iceberg=default_spark_spec.support_iceberg, support_hudi=default_spark_spec.support_hudi)

        print("\n" + "="*70)
        print(f"TEST: skipSparkSuffix=true (backward compatibility - no suffix)")
        print("="*70)

        self.clean_maven_cache()

        if not self.run_sbt_command(
            "Running: build/sbt -DskipSparkSuffix=true publishM2",
            ["build/sbt", "-DskipSparkSuffix=true", "publishM2"]
        ):
            return False

        # Expect artifacts WITHOUT suffix (e.g., delta-spark_2.13 instead of delta-spark_4.0_2.13)
        expected = substitute_xversion(spark_spec_no_suffix.all_jars, self.delta_version)
        return self.validate_jars(expected, "skipSparkSuffix=true (backward compat)")

    def test_cross_spark_workflow(self) -> bool:
        """Full cross-Spark workflow: backward-compat (no suffix) + all versions (with suffix)."""
        print("\n" + "="*70)
        print("TEST: Cross-Spark Workflow (backward-compat + all non-master with suffix)")
        print("="*70)

        self.clean_maven_cache()

        # Step 1: Publish all modules WITHOUT suffix (backward compatibility)
        if not self.run_sbt_command(
            "Step 1: build/sbt -DskipSparkSuffix=true publishM2 (backward compat, no suffix)",
            ["build/sbt", "-DskipSparkSuffix=true", "publishM2"]
        ):
            return False

        # Step 2: Publish Spark-dependent modules WITH suffix for each non-master version
        for spark_version, spark_spec in SPARK_VERSIONS.items():
            # Skip master/snapshot versions
            if "SNAPSHOT" in spark_version:
                continue

            if not self.run_sbt_command(
                f"Step 2: build/sbt -DsparkVersion={spark_version} \"runOnlyForReleasableSparkModules publishM2\" (with suffix)",
                ["build/sbt", f"-DsparkVersion={spark_version}", "runOnlyForReleasableSparkModules publishM2"]
            ):
                return False

        # Build expected JARs:
        # 1. All modules WITHOUT suffix (from Step 1 - backward compat)
        # 2. Spark-dependent modules WITH suffix for each non-master version (from Step 2)
        # 3. Iceberg/Hudi JARs for supported versions (no Spark suffix)
        expected = set()

        # Step 1: All modules without suffix (uses default Spark version's iceberg support)
        default_spark_spec = SPARK_VERSIONS[DEFAULT_SPARK]
        no_suffix_spec = SparkVersionSpec(suffix="", support_iceberg=default_spark_spec.support_iceberg, support_hudi=default_spark_spec.support_hudi)
        expected.update(substitute_xversion(no_suffix_spec.all_jars, self.delta_version))

        # Step 2: Spark-dependent modules WITH suffix for each non-master version
        for spark_version, spark_spec in SPARK_VERSIONS.items():
            if "SNAPSHOT" in spark_version:
                continue  # Skip master/snapshot

            expected.update(substitute_xversion(spark_spec.spark_related_jars, self.delta_version))
            expected.update(substitute_xversion(spark_spec.iceberg_jars, self.delta_version))
            expected.update(substitute_xversion(spark_spec.hudi_jars, self.delta_version))

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


class SparkVersionsScriptTest:
    """Tests for the get_spark_version_info.py script."""

    def __init__(self, delta_root: Path):
        self.delta_root = delta_root
        self.json_path = delta_root / "target" / "spark-versions.json"
        self.script_path = delta_root / "project" / "scripts" / "get_spark_version_info.py"

    def ensure_json_exists(self) -> bool:
        """Ensure the JSON file exists by running exportSparkVersionsJson."""
        if not self.json_path.exists():
            print("  Generating spark-versions.json...")
            try:
                subprocess.run(
                    ["build/sbt", "exportSparkVersionsJson"],
                    cwd=self.delta_root,
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.STDOUT
                )
            except subprocess.CalledProcessError:
                print("  ✗ Failed to generate spark-versions.json")
                return False
        return True

    def test_json_format(self) -> bool:
        """Test that the JSON file is well-formed with expected fields."""
        if not self.ensure_json_exists():
            return False

        try:
            with open(self.json_path, 'r') as f:
                data = json.load(f)

            # Validate it's an array
            if not isinstance(data, list) or len(data) == 0:
                print("  ✗ JSON must be a non-empty array")
                return False

            # Validate each entry has required fields
            required_fields = ["fullVersion", "shortVersion", "isMaster", "isDefault", "targetJvm", "packageSuffix"]
            for idx, entry in enumerate(data):
                for field in required_fields:
                    if field not in entry:
                        print(f"  ✗ Entry {idx} missing required field: {field}")
                        return False

                # Validate field types
                if not isinstance(entry["fullVersion"], str) or not isinstance(entry["shortVersion"], str) or \
                   not isinstance(entry["isMaster"], bool) or not isinstance(entry["isDefault"], bool) or \
                   not isinstance(entry["targetJvm"], str) or not isinstance(entry["packageSuffix"], str):
                    print(f"  ✗ Entry {idx}: Invalid field types")
                    return False

            versions_str = ", ".join([entry.get("isMaster") and "master" or entry["shortVersion"] for entry in data])
            print(f"  ✓ JSON format valid: {len(data)} version(s) [{versions_str}]")
            return True

        except json.JSONDecodeError as e:
            print(f"  ✗ Invalid JSON: {e}")
            return False
        except Exception as e:
            print(f"  ✗ Unexpected error: {e}")
            return False

    def test_all_spark_versions(self) -> bool:
        """Test that --all-spark-versions produces valid JSON array."""
        if not self.ensure_json_exists():
            return False

        try:
            result = subprocess.run(
                ["python3", str(self.script_path), "--all-spark-versions"],
                cwd=self.delta_root,
                capture_output=True,
                text=True,
                check=True
            )

            matrix_versions = json.loads(result.stdout.strip())

            # Validate it's a non-empty array of strings
            if not isinstance(matrix_versions, list) or len(matrix_versions) == 0:
                print("  ✗ Must output a non-empty JSON array")
                return False

            if not all(isinstance(v, str) for v in matrix_versions):
                print("  ✗ All matrix entries must be strings")
                return False

            # Validate consistency with JSON
            with open(self.json_path, 'r') as f:
                data = json.load(f)

            if len(matrix_versions) != len(data):
                print(f"  ✗ Matrix has {len(matrix_versions)} versions, JSON has {len(data)}")
                return False

            print(f"  ✓ --all-spark-versions: {matrix_versions}")
            return True

        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            print(f"  ✗ Failed: {e}")
            return False

    def test_released_spark_versions(self) -> bool:
        """Test that --released-spark-versions excludes snapshots."""
        if not self.ensure_json_exists():
            return False

        try:
            result = subprocess.run(
                ["python3", str(self.script_path), "--released-spark-versions"],
                cwd=self.delta_root,
                capture_output=True,
                text=True,
                check=True
            )

            released_versions = json.loads(result.stdout.strip())

            # Validate it's an array of strings
            if not isinstance(released_versions, list):
                print("  ✗ Must output a JSON array")
                return False

            if not all(isinstance(v, str) for v in released_versions):
                print("  ✗ All entries must be strings")
                return False

            # Load JSON and verify snapshots are excluded
            with open(self.json_path, 'r') as f:
                data = json.load(f)

            expected_count = sum(1 for entry in data if "-SNAPSHOT" not in entry["fullVersion"])
            if len(released_versions) != expected_count:
                print(f"  ✗ Expected {expected_count} released versions, got {len(released_versions)}")
                return False

            # Verify no snapshot versions included
            for version in released_versions:
                if "SNAPSHOT" in version.upper():
                    print(f"  ✗ Released versions should not include snapshots: {version}")
                    return False

            print(f"  ✓ --released-spark-versions: {released_versions} (snapshots excluded)")
            return True

        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            print(f"  ✗ Failed: {e}")
            return False

    def test_get_field(self) -> bool:
        """Test that --get-field works for various version formats."""
        if not self.ensure_json_exists():
            return False

        try:
            # Load the JSON to know what versions to test
            with open(self.json_path, 'r') as f:
                data = json.load(f)

            test_cases = []
            for entry in data:
                # Test short version and full version
                test_cases.append((entry["shortVersion"], "targetJvm", entry["targetJvm"]))
                test_cases.append((entry["fullVersion"], "fullVersion", entry["fullVersion"]))
                
                # Test "master" if applicable
                if entry["isMaster"]:
                    test_cases.append(("master", "targetJvm", entry["targetJvm"]))

            all_passed = True
            for version, field, expected in test_cases:
                result = subprocess.run(
                    ["python3", str(self.script_path), "--get-field", version, field],
                    cwd=self.delta_root,
                    capture_output=True,
                    text=True,
                    check=True
                )

                actual = json.loads(result.stdout.strip())
                if actual != expected:
                    print(f"  ✗ --get-field {version} {field}: expected {expected}, got {actual}")
                    all_passed = False

            if all_passed:
                print(f"  ✓ --get-field: Tested {len(test_cases)} cases successfully")
            return all_passed

        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            print(f"  ✗ Failed: {e}")
            return False


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

        # Test the get_spark_version_info.py script first
        print("\n" + "="*70)
        print("PART 1: Spark Versions Script Tests")
        print("="*70)
        script_test = SparkVersionsScriptTest(delta_root)
        script_test1_passed = script_test.test_json_format()
        script_test2_passed = script_test.test_all_spark_versions()
        script_test3_passed = script_test.test_released_spark_versions()
        script_test4_passed = script_test.test_get_field()

        # Test cross-Spark build workflow
        print("\n" + "="*70)
        print("PART 2: Cross-Spark Build Tests")
        print("="*70)
        build_test = CrossSparkPublishTest(delta_root)
        build_test.validate_spark_versions()

        # Run all build tests
        build_test1_passed = build_test.test_default_publish()
        build_test2_passed = build_test.test_backward_compat_publish()
        build_test3_passed = build_test.test_cross_spark_workflow()

        # Summary
        print("\n" + "="*70)
        print("TEST SUMMARY")
        print("="*70)
        print("\nPart 1: Spark Versions Script Tests")
        print(f"  JSON Format:                            {'✓ PASSED' if script_test1_passed else '✗ FAILED'}")
        print(f"  All Spark Versions Output:              {'✓ PASSED' if script_test2_passed else '✗ FAILED'}")
        print(f"  Released Spark Versions Output:         {'✓ PASSED' if script_test3_passed else '✗ FAILED'}")
        print(f"  Get Field Functionality:                {'✓ PASSED' if script_test4_passed else '✗ FAILED'}")
        print("\nPart 2: Cross-Spark Build Tests")
        print(f"  Default publishM2 (with suffix):        {'✓ PASSED' if build_test1_passed else '✗ FAILED'}")
        print(f"  skipSparkSuffix (backward compat):      {'✓ PASSED' if build_test2_passed else '✗ FAILED'}")
        print(f"  Cross-Spark Workflow (both):            {'✓ PASSED' if build_test3_passed else '✗ FAILED'}")
        print("="*70)

        all_tests_passed = (
            script_test1_passed and script_test2_passed and script_test3_passed and script_test4_passed and
            build_test1_passed and build_test2_passed and build_test3_passed
        )

        if all_tests_passed:
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
