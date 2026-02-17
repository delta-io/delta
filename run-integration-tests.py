#!/usr/bin/env python3

#
# Copyright (2021) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import subprocess
from os import path
import shutil
import argparse
import json


_original_path = os.environ.get("PATH", "")


def set_spark_env(spark_version):
    """
    Sets SPARK_HOME and prepends its bin/ to PATH for the given Spark version.
    Resets PATH to its original value first to avoid accumulation.
    For empty or SNAPSHOT versions, just resets PATH (uses whatever is on PATH).
    """
    os.environ["PATH"] = _original_path

    if not spark_version or "-SNAPSHOT" in spark_version:
        return

    spark_home = os.path.expanduser("~/spark-%s-bin-hadoop3" % spark_version)
    if not os.path.isdir(spark_home):
        raise Exception(
            "Spark %s not found at %s. Please download it first:\n"
            "  wget https://archive.apache.org/dist/spark/spark-%s/spark-%s-bin-hadoop3.tgz\n"
            "  tar xzf spark-%s-bin-hadoop3.tgz -C ~/"
            % (spark_version, spark_home, spark_version, spark_version, spark_version))

    os.environ["SPARK_HOME"] = spark_home
    spark_bin = os.path.join(spark_home, "bin")
    os.environ["PATH"] = spark_bin + os.pathsep + _original_path
    print("Using SPARK_HOME=%s" % spark_home)


def delete_if_exists(path):
    # if path exists, delete it.
    if os.path.exists(path):
        shutil.rmtree(path)
        print("Deleted %s " % path)


def load_spark_version_specs(root_dir):
    """
    Loads Spark version specs from target/spark-versions.json (single source of truth).
    Runs `build/sbt exportSparkVersionsJson` if the file doesn't exist yet.
    Returns a list of dicts with keys: fullVersion, shortVersion, isMaster, isDefault,
    targetJvm, packageSuffix, supportIceberg, supportHudi.
    """
    json_path = path.join(root_dir, "target", "spark-versions.json")
    if not path.exists(json_path):
        print("Generating %s via exportSparkVersionsJson..." % json_path)
        run_cmd(["build/sbt", "exportSparkVersionsJson"], stream_output=True)
    with open(json_path) as f:
        return json.load(f)


def publish_all_variants(root_dir, spark_specs):
    """
    Publishes all artifact variants once upfront (replaces per-function publishM2 calls).

    Step 1: Publish all modules WITHOUT Spark suffix (backward compatibility)
    Step 2: Publish Spark-dependent modules WITH suffix for each non-master Spark version
    """
    # Step 1: unsuffixed (backward compat)
    print("\n##### Publishing all modules without Spark suffix (backward compat) #####")
    run_cmd(["build/sbt", "-DskipSparkSuffix=true", "publishM2"], stream_output=True)

    # Step 2: suffixed for each non-master Spark version
    # Clean between publishes to avoid stale class files from different Spark shims
    for spec in spark_specs:
        if spec.get("isMaster", False):
            continue
        spark_version = spec["fullVersion"]
        print("\n##### Publishing Spark-dependent modules for Spark %s #####" % spark_version)
        run_cmd(
            ["build/sbt", "-DsparkVersion=%s" % spark_version,
             "runOnlyForReleasableSparkModules clean",
             "runOnlyForReleasableSparkModules publishM2"],
            stream_output=True)


def get_spark_variants(spark_specs):
    """
    Builds the list of artifact variants to test from the Spark version specs.

    Each variant is a dict with:
      - suffix: Maven artifact suffix, e.g. "" (unsuffixed), "_4.0", "_4.1"
      - spark_version: full Spark version, e.g. "4.1.0", "4.0.1"
      - support_iceberg: "true" or "false"
      - support_hudi: "true" or "false"

    The first variant is always unsuffixed (backward compat) using the DEFAULT spec's metadata.
    Remaining variants are suffixed, one per non-master Spark version.

    Example return value (given Spark 4.0 and 4.1 specs, with 4.1 as default):
      [
        {"suffix": "",     "spark_version": "4.1.0", "support_iceberg": "false", "support_hudi": "false"},
        {"suffix": "_4.0", "spark_version": "4.0.1", "support_iceberg": "true",  "support_hudi": "true"},
        {"suffix": "_4.1", "spark_version": "4.1.0", "support_iceberg": "false", "support_hudi": "false"},
      ]
    """
    variants = []

    # Find the default spec for the unsuffixed backward-compat variant
    default_spec = None
    for spec in spark_specs:
        if spec.get("isDefault", False):
            default_spec = spec
            break
    if default_spec is None and spark_specs:
        default_spec = spark_specs[-1]  # fallback to last spec

    # Unsuffixed variant (backward compat) - uses default's metadata
    if default_spec:
        variants.append({
            "suffix": "",
            "spark_version": default_spec["fullVersion"],
            "support_iceberg": default_spec.get("supportIceberg", "false"),
            "support_hudi": default_spec.get("supportHudi", "false"),
        })

    # Suffixed variants for each non-master spec
    for spec in spark_specs:
        if spec.get("isMaster", False):
            continue
        variants.append({
            "suffix": spec["packageSuffix"],
            "spark_version": spec["fullVersion"],
            "support_iceberg": spec.get("supportIceberg", "false"),
            "support_hudi": spec.get("supportHudi", "false"),
        })

    return variants


def run_scala_integration_tests(root_dir, version, test_name, extra_maven_repo, scala_version,
                                variant):
    """
    Runs Scala integration tests for a single artifact variant.

    variant: dict with suffix, spark_version, support_iceberg, support_hudi.
             See get_spark_variants() for the format and example.
    """
    suffix = variant["suffix"]
    spark_version = variant["spark_version"]
    support_iceberg = variant["support_iceberg"]
    label = " (suffix=%s, spark=%s)" % (suffix or "none", spark_version) if suffix or spark_version else ""

    print("\n\n##### Running Scala tests%s on delta %s, scala %s #####"
          % (label, str(version), scala_version))

    test_dir = path.join(root_dir, "examples", "scala")
    test_src_dir = path.join(test_dir, "src", "main", "scala", "example")
    test_classes = [f.replace(".scala", "") for f in os.listdir(test_src_dir)
                    if f.endswith(".scala") and not f.startswith("_")]

    # Set env vars that examples/scala/build.sbt reads to resolve dependencies:
    # SPARK_PACKAGE_SUFFIX -> artifact suffix (e.g., "_4.0")
    # SPARK_VERSION -> Spark version for spark-sql/spark-hive deps (e.g., "4.0.1")
    # SUPPORT_ICEBERG -> whether to include Iceberg deps and compile IcebergCompat examples
    env = {"DELTA_VERSION": str(version), "SCALA_VERSION": scala_version}
    if suffix:
        env["SPARK_PACKAGE_SUFFIX"] = suffix
    if spark_version:
        env["SPARK_VERSION"] = spark_version
    if support_iceberg == "true":
        env["SUPPORT_ICEBERG"] = "true"
    if extra_maven_repo:
        env["EXTRA_MAVEN_REPO"] = extra_maven_repo

    with WorkingDirectory(test_dir):
        for test_class in test_classes:
            if test_name is not None and test_name not in test_class:
                print("\nSkipping Scala tests in %s\n=====================" % test_class)
                continue

            # Skip Iceberg tests for variants that don't support Iceberg
            if "IcebergCompat" in test_class and support_iceberg != "true":
                print("\nSkipping %s (Iceberg not supported for this variant)\n=====================" % test_class)
                continue

            try:
                cmd = ["build/sbt", "runMain example.%s" % test_class]
                print("\nRunning Scala tests in %s%s\n=====================" % (test_class, label))
                print("Command: %s" % " ".join(cmd))
                run_cmd(cmd, stream_output=True, env=env)
            except:
                print("Failed Scala tests in %s%s" % (test_class, label))
                raise


def get_artifact_name(version):
    """
    version: string representation, e.g. 2.3.0 or 3.0.0.rc1
    return: either "core" or "spark"
    """
    return "spark" if int(version[0]) >= 3 else "core"


def run_python_integration_tests(root_dir, version, test_name, extra_maven_repo, variant):
    """
    Runs Python integration tests for a single artifact variant.

    variant: dict with suffix, spark_version, support_iceberg, support_hudi.
             See get_spark_variants() for the format and example.
    """
    suffix = variant["suffix"]
    label = " (suffix=%s)" % (suffix or "none") if suffix else ""

    print("\n\n##### Running Python tests%s on version %s #####" % (label, str(version)))

    test_dir = path.join(root_dir, path.join("examples", "python"))
    files_to_skip = {"using_with_pip.py", "missing_delta_storage_jar.py", "image_storage.py", "delta_connect.py"}

    test_files = [path.join(test_dir, f) for f in os.listdir(test_dir)
                  if path.isfile(path.join(test_dir, f)) and
                  f.endswith(".py") and not f.startswith("_") and
                  f not in files_to_skip]

    python_root_dir = path.join(root_dir, "python")
    extra_class_path = path.join(python_root_dir, path.join("delta", "testing"))
    repo = extra_maven_repo if extra_maven_repo else ""

    # Build Maven coordinate with the variant's suffix
    # e.g., "io.delta:delta-spark_2.13:4.0.0" or "io.delta:delta-spark_4.0_2.13:4.0.0"
    artifact_name = get_artifact_name(version)
    package = "io.delta:delta-%s%s_2.13:%s" % (artifact_name, suffix, version)
    print("Package: %s" % package)

    for test_file in test_files:
        if test_name is not None and test_name not in test_file:
            print("\nSkipping Python tests in %s\n=====================" % test_file)
            continue
        try:
            cmd = ["spark-submit",
                   "--driver-class-path=%s" % extra_class_path,  # for less verbose logging
                   "--packages", package,
                   "--repositories", repo, test_file]
            print("\nRunning Python tests in %s%s\n=============" % (test_file, label))
            print("Command: %s" % " ".join(cmd))
            run_cmd(cmd, stream_output=True)
        except:
            print("Failed Python tests in %s%s" % (test_file, label))
            raise


def test_missing_delta_storage_jar(root_dir, version, use_local):
    if not use_local:
        print("Skipping 'missing_delta_storage_jar' - test should only run in local mode")
        return

    print("\n\n##### Running 'missing_delta_storage_jar' on version %s #####" % str(version))

    # The unsuffixed artifact was published via publish_all_variants upfront.
    # Clear only the delta-storage artifact to test the missing JAR scenario.
    print("Clearing delta-storage artifact")
    delete_if_exists(os.path.expanduser("~/.m2/repository/io/delta/delta-storage"))
    delete_if_exists(os.path.expanduser("~/.ivy2/cache/io.delta/delta-storage"))
    delete_if_exists(os.path.expanduser("~/.ivy2/local/io.delta/delta-storage"))
    delete_if_exists(os.path.expanduser("~/.ivy2.5.2/local/io.delta/delta-storage"))
    delete_if_exists(os.path.expanduser("~/.ivy2.5.2/cache/io.delta/delta-storage"))

    python_root_dir = path.join(root_dir, "python")
    extra_class_path = path.join(python_root_dir, path.join("delta", "testing"))
    test_file = path.join(root_dir, path.join("examples", "python", "missing_delta_storage_jar.py"))
    artifact_name = get_artifact_name(version)
    # Uses unsuffixed artifact name (published via -DskipSparkSuffix=true)
    jar = path.join(
        os.path.expanduser("~/.m2/repository/io/delta/"),
        "delta-%s_2.13" % artifact_name,
        version,
        "delta-%s_2.13-%s.jar" % (artifact_name, str(version)))

    try:
        cmd = ["spark-submit",
               "--driver-class-path=%s" % extra_class_path,  # for less verbose logging
               "--jars", jar, test_file]
        print("\nRunning Python tests in %s\n=============" % test_file)
        print("Command: %s" % " ".join(cmd))
        run_cmd(cmd, stream_output=True)
    except:
        print("Failed Python tests in %s" % (test_file))
        raise


def run_dynamodb_logstore_integration_tests(root_dir, version, test_name, extra_maven_repo,
                                            extra_packages, conf, variant):
    """
    Runs DynamoDB logstore integration tests for a single artifact variant.

    variant: dict with suffix, spark_version, support_iceberg, support_hudi.
             See get_spark_variants() for the format and example.
    """
    suffix = variant["suffix"]
    label = " (suffix=%s)" % (suffix or "none") if suffix else ""

    print(
        "\n\n##### Running DynamoDB logstore integration tests%s on version %s #####"
        % (label, str(version))
    )

    test_dir = path.join(root_dir, path.join("storage-s3-dynamodb", "integration_tests"))
    test_files = [path.join(test_dir, f) for f in os.listdir(test_dir)
                  if path.isfile(path.join(test_dir, f)) and
                  f.endswith(".py") and not f.startswith("_")]

    python_root_dir = path.join(root_dir, "python")
    extra_class_path = path.join(python_root_dir, path.join("delta", "testing"))

    conf_args = []
    if conf:
        for i in conf:
            conf_args.extend(["--conf", i])

    repo_args = ["--repositories", extra_maven_repo] if extra_maven_repo else []

    # Build package string: delta-spark with suffix + delta-storage-s3-dynamodb (Spark-independent, no suffix)
    artifact_name = get_artifact_name(version)
    packages = "io.delta:delta-%s%s_2.13:%s" % (artifact_name, suffix, version)
    packages += "," + "io.delta:delta-storage-s3-dynamodb:" + version
    if extra_packages:
        packages += "," + extra_packages

    for test_file in test_files:
        if test_name is not None and test_name not in test_file:
            print("\nSkipping DynamoDB logstore integration tests in %s\n============" % test_file)
            continue
        try:
            cmd = ["spark-submit",
                   "--driver-class-path=%s" % extra_class_path,  # for less verbose logging
                   "--packages", packages] + repo_args + conf_args + [test_file]
            print("\nRunning DynamoDB logstore integration tests in %s%s\n=============" % (test_file, label))
            print("Command: %s" % " ".join(cmd))
            run_cmd(cmd, stream_output=True)
        except:
            print("Failed DynamoDB logstore integration tests tests in %s%s" % (test_file, label))
            raise

def run_dynamodb_commit_coordinator_integration_tests(root_dir, version, test_name, extra_maven_repo,
                                                extra_packages, conf, variant):
    """
    Runs DynamoDB Commit Coordinator integration tests for a single artifact variant.

    variant: dict with suffix, spark_version, support_iceberg, support_hudi.
             See get_spark_variants() for the format and example.
    """
    suffix = variant["suffix"]
    label = " (suffix=%s)" % (suffix or "none") if suffix else ""

    print(
        "\n\n##### Running DynamoDB Commit Coordinator integration tests%s on version %s #####"
        % (label, str(version))
    )

    test_dir = path.join(root_dir, \
        path.join("spark", "src", "main", "java", "io", "delta", "dynamodbcommitcoordinator", "integration_tests"))
    test_files = [path.join(test_dir, f) for f in os.listdir(test_dir)
                  if path.isfile(path.join(test_dir, f)) and
                  f.endswith(".py") and not f.startswith("_")]

    python_root_dir = path.join(root_dir, "python")
    extra_class_path = path.join(python_root_dir, path.join("delta", "testing"))

    conf_args = []
    if conf:
        for i in conf:
            conf_args.extend(["--conf", i])

    repo_args = ["--repositories", extra_maven_repo] if extra_maven_repo else []

    # Build package string with the variant's suffix
    artifact_name = get_artifact_name(version)
    packages = "io.delta:delta-%s%s_2.13:%s" % (artifact_name, suffix, version)
    if extra_packages:
        packages += "," + extra_packages

    for test_file in test_files:
        if test_name is not None and test_name not in test_file:
            print("\nSkipping DynamoDB Commit Coordinator integration tests in %s\n============" % test_file)
            continue
        try:
            cmd = ["spark-submit",
                   "--driver-class-path=%s" % extra_class_path,  # for less verbose logging
                   "--packages", packages] + repo_args + conf_args + [test_file]
            print("\nRunning DynamoDB Commit Coordinator integration tests in %s%s\n=============" % (test_file, label))
            print("Command: %s" % " ".join(cmd))
            run_cmd(cmd, stream_output=True)
        except:
            print("Failed DynamoDB Commit Coordinator integration tests in %s%s" % (test_file, label))
            raise

def run_s3_log_store_util_integration_tests():
    print("\n\n##### Running S3LogStoreUtil tests #####")

    env = { "S3_LOG_STORE_UTIL_TEST_ENABLED": "true" }
    assert os.environ.get("S3_LOG_STORE_UTIL_TEST_BUCKET") is not None, "S3_LOG_STORE_UTIL_TEST_BUCKET must be set"
    assert os.environ.get("S3_LOG_STORE_UTIL_TEST_RUN_UID") is not None, "S3_LOG_STORE_UTIL_TEST_RUN_UID must be set"

    try:
        cmd = ["build/sbt", "project storage", "testOnly -- -n IntegrationTest"]
        print("\nRunning IntegrationTests of storage\n=====================")
        print("Command: %s" % " ".join(cmd))
        run_cmd(cmd, stream_output=True, env=env)
    except:
        print("Failed IntegrationTests")
        raise


def run_iceberg_integration_tests(root_dir, version, iceberg_version, extra_maven_repo, variant):
    """
    Runs Iceberg integration tests for a single artifact variant.

    variant: dict with suffix, spark_version, support_iceberg, support_hudi.
             See get_spark_variants() for the format and example.
             spark_version is used to derive the iceberg-spark-runtime artifact name
             (e.g., "4.0.1" -> iceberg-spark-runtime-4.0_2.13).
    """
    suffix = variant["suffix"]
    spark_version = variant["spark_version"]
    label = " (suffix=%s)" % (suffix or "none") if suffix else ""

    print("\n\n##### Running Iceberg tests%s on version %s #####" % (label, str(version)))

    test_dir = path.join(root_dir, path.join("iceberg", "integration_tests"))

    # Add more Iceberg tests here if needed ...
    test_files_names = ["iceberg_converter.py"]
    test_files = [path.join(test_dir, f) for f in test_files_names]

    python_root_dir = path.join(root_dir, "python")
    extra_class_path = path.join(python_root_dir, path.join("delta", "testing"))
    repo = extra_maven_repo if extra_maven_repo else ""

    artifact_name = get_artifact_name(version)

    # Derive major.minor Spark version for iceberg-spark-runtime artifact name
    # e.g., "4.0.1" -> "4.0", or "4.0" stays "4.0"
    parts = spark_version.split(".")
    iceberg_spark_ver = "%s.%s" % (parts[0], parts[1]) if len(parts) >= 2 else spark_version

    # Build package string with suffixed Delta artifacts + Iceberg runtime
    package = ','.join([
        "io.delta:delta-%s%s_2.13:%s" % (artifact_name, suffix, version),
        "io.delta:delta-iceberg_2.13:%s" % (version),
        "org.apache.iceberg:iceberg-spark-runtime-{}_2.13:{}".format(iceberg_spark_ver, iceberg_version)])

    print("Package: %s" % package)

    for test_file in test_files:
        try:
            cmd = ["spark-submit",
                   "--driver-class-path=%s" % extra_class_path,  # for less verbose logging
                   "--packages", package,
                   "--repositories", repo, test_file]
            print("\nRunning Iceberg tests in %s%s\n=============" % (test_file, label))
            print("Command: %s" % " ".join(cmd))
            run_cmd(cmd, stream_output=True)
        except:
            print("Failed Iceberg tests in %s%s" % (test_file, label))
            raise

def run_uniform_hudi_integration_tests(root_dir, version, hudi_version, extra_maven_repo, variant):
    """
    Runs Uniform Hudi integration tests for a single artifact variant.

    variant: dict with suffix, spark_version, support_iceberg, support_hudi.
             See get_spark_variants() for the format and example.
             spark_version is used to derive the hudi-spark-bundle artifact name
             (e.g., "4.0.1" -> hudi-spark4.0-bundle_2.13).
    """
    suffix = variant["suffix"]
    spark_version = variant["spark_version"]
    label = " (suffix=%s)" % (suffix or "none") if suffix else ""

    print("\n\n##### Running Uniform hudi tests%s on version %s #####" % (label, str(version)))

    test_dir = path.join(root_dir, path.join("hudi", "integration_tests"))

    # Add more tests here if needed ...
    test_files_names = ["write_uniform_hudi.py"]
    test_files = [path.join(test_dir, f) for f in test_files_names]

    python_root_dir = path.join(root_dir, "python")
    extra_class_path = path.join(python_root_dir, path.join("delta", "testing"))
    # The hudi assembly JAR path uses name.value (no suffix), not moduleName
    jars = path.join(root_dir, "hudi/target/scala-2.13/delta-hudi-assembly_2.13-%s.jar" % (version))
    repo = extra_maven_repo if extra_maven_repo else ""

    artifact_name = get_artifact_name(version)

    # Derive major.minor Spark version for hudi-spark-bundle artifact name
    # e.g., "4.0.1" -> "4.0", or "4.0" stays "4.0"
    parts = spark_version.split(".")
    hudi_spark_ver = "%s.%s" % (parts[0], parts[1]) if len(parts) >= 2 else spark_version

    # Build package string with suffixed Delta artifact + Hudi bundle
    package = ','.join([
        "io.delta:delta-%s%s_2.13:%s" % (artifact_name, suffix, version),
        "org.apache.hudi:hudi-spark%s-bundle_2.13:%s" % (hudi_spark_ver, hudi_version)
    ])

    print("Package: %s" % package)

    for test_file in test_files:
        try:
            cmd = ["spark-submit",
                   "--driver-class-path=%s" % extra_class_path,  # for less verbose logging
                   "--packages", package,
                   "--jars", jars,
                   "--repositories", repo, test_file]
            print("\nRunning Uniform Hudi tests in %s%s\n=============" % (test_file, label))
            print("Command: %s" % " ".join(cmd))
            run_cmd(cmd, stream_output=True)
        except:
            print("Failed Uniform Hudi tests in %s%s" % (test_file, label))
            raise

def run_pip_installation_tests(root_dir, version, use_testpypi, use_localpypi, extra_maven_repo):
    print("\n\n##### Running pip installation tests on version %s #####" % str(version))
    # Note: no clear_artifact_cache() here. Pip tests install from PyPI, not local M2.
    delta_pip_name = "delta-spark"
    # uninstall packages if they exist
    run_cmd(["pip", "uninstall", "--yes", delta_pip_name, "pyspark"], stream_output=True)

    # install packages
    delta_pip_name_with_version = "%s==%s" % (delta_pip_name, str(version))
    if use_testpypi:
        install_cmd = ["pip", "install",
                       "--extra-index-url", "https://test.pypi.org/simple/",
                       delta_pip_name_with_version]
    elif use_localpypi:
        pip_wheel_file_name = "%s-%s-py3-none-any.whl" % \
                              (delta_pip_name.replace("-", "_"), str(version))
        pip_wheel_file_path = os.path.join(use_localpypi, pip_wheel_file_name)
        install_cmd = ["pip", "install", pip_wheel_file_path]
    else:
        install_cmd = ["pip", "install", delta_pip_name_with_version]
    print("pip install command: %s" % str(install_cmd))
    run_cmd(install_cmd, stream_output=True)

    # run test python file directly with python and not with spark-submit
    env = {}
    if extra_maven_repo:
        env["EXTRA_MAVEN_REPO"] = extra_maven_repo
    tests = ["image_storage.py", "using_with_pip.py"]
    for test in tests:
        test_file = path.join(root_dir, path.join("examples", "python", test))
        print("\nRunning Python tests in %s\n=============" % test_file)
        test_cmd = ["python3", test_file]
        print("Test command: %s" % str(test_cmd))
        try:
            run_cmd(test_cmd, stream_output=True, env=env)
        except:
            print("Failed pip installation tests in %s" % (test_file))
            raise

def run_unity_catalog_commit_coordinator_integration_tests(root_dir, version, test_name,
                                                           variant, extra_packages):
    """
    Runs Unity Catalog commit coordinator integration tests for a single artifact variant.

    variant: dict with suffix, spark_version, support_iceberg, support_hudi.
             See get_spark_variants() for the format and example.
    """
    suffix = variant["suffix"]
    label = " (suffix=%s)" % (suffix or "none") if suffix else ""

    print(
        "\n\n##### Running Unity Catalog commit coordinator integration tests%s on version %s #####"
        % (label, str(version))
    )

    test_dir = path.join(root_dir, \
        path.join("python", "delta", "integration_tests"))
    test_files = [path.join(test_dir, f) for f in os.listdir(test_dir)
                  if path.isfile(path.join(test_dir, f)) and
                  f.endswith(".py") and not f.startswith("_")]

    print("\n\nTests compiled\n\n")

    python_root_dir = path.join(root_dir, "python")
    extra_class_path = path.join(python_root_dir, path.join("delta", "testing"))

    # Build package string with the variant's suffix
    artifact_name = get_artifact_name(version)
    packages = "io.delta:delta-%s%s_2.13:%s" % (artifact_name, suffix, version)
    if extra_packages:
        packages += "," + extra_packages

    for test_file in test_files:
        if test_name is not None and test_name not in test_file:
            print("\nSkipping Unity Catalog commit coordinator integration tests in %s\n============" % test_file)
            continue
        try:
            cmd = ["spark-submit",
                   "--driver-class-path=%s" % extra_class_path,  # for less verbose logging
                   "--packages", packages] + [test_file]
            print("\nRunning External uc managed tables integration tests in %s%s\n=============" % (test_file, label))
            print("Command: %s" % " ".join(cmd))
            run_cmd(cmd, stream_output=True)
        except:
            print("Failed Unity Catalog commit coordinator integration tests in %s%s" % (test_file, label))
            raise

def clear_artifact_cache():
    print("Clearing Delta artifacts from ivy2 and mvn cache")
    ivy_caches_to_clear = [filepath for filepath in os.listdir(os.path.expanduser("~")) if filepath.startswith(".ivy")]
    print(f"Clearing Ivy caches in: {ivy_caches_to_clear}")
    for filepath in ivy_caches_to_clear:
        delete_if_exists(os.path.expanduser(f"~/{filepath}/cache/io.delta"))
        delete_if_exists(os.path.expanduser(f"~/{filepath}/local/io.delta"))
    delete_if_exists(os.path.expanduser("~/.m2/repository/io/delta/"))


def run_cmd(cmd, throw_on_error=True, env=None, stream_output=False, **kwargs):
    cmd_env = os.environ.copy()
    if env:
        cmd_env.update(env)

    if stream_output:
        child = subprocess.Popen(cmd, env=cmd_env, **kwargs)
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception("Non-zero exitcode: %s" % (exit_code))
        return exit_code
    else:
        child = subprocess.Popen(
            cmd,
            env=cmd_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **kwargs)
        (stdout, stderr) = child.communicate()
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception(
                "Non-zero exitcode: %s\n\nSTDOUT:\n%s\n\nSTDERR:%s" %
                (exit_code, stdout, stderr))
        return (exit_code, stdout, stderr)


# pylint: disable=too-few-public-methods
class WorkingDirectory(object):
    def __init__(self, working_directory):
        self.working_directory = working_directory
        self.old_workdir = os.getcwd()

    def __enter__(self):
        os.chdir(self.working_directory)

    def __exit__(self, tpe, value, traceback):
        os.chdir(self.old_workdir)


if __name__ == "__main__":
    """
        Script to run integration tests which are located in the examples directory.
        call this by running "python run-integration-tests.py"
        additionally the version can be provided as a command line argument.
        "
    """

    # get the version of the package
    root_dir = path.dirname(__file__)
    with open(path.join(root_dir, "version.sbt")) as fd:
        default_version = fd.readline().split('"')[1]

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--version",
        required=False,
        default=default_version,
        help="Delta version to use to run the integration tests")
    parser.add_argument(
        "--python-only",
        required=False,
        default=False,
        action="store_true",
        help="Run only Python tests")
    parser.add_argument(
        "--scala-only",
        required=False,
        default=False,
        action="store_true",
        help="Run only Scala tests")
    parser.add_argument(
        "--s3-log-store-util-only",
        required=False,
        default=False,
        action="store_true",
        help="Run only S3LogStoreUtil tests")
    parser.add_argument(
        "--scala-version",
        required=False,
        default="2.13",
        help="Specify scala version for scala tests only, valid values are '2.13'")
    parser.add_argument(
        "--pip-only",
        required=False,
        default=False,
        action="store_true",
        help="Run only pip installation tests")
    parser.add_argument(
        "--no-pip",
        required=False,
        default=False,
        action="store_true",
        help="Do not run pip installation tests")
    parser.add_argument(
        "--test",
        required=False,
        default=None,
        help="Run a specific test by substring-match with Scala/Python file name")
    parser.add_argument(
        "--maven-repo",
        required=False,
        default=None,
        help="Additional Maven repo to resolve staged new release artifacts")
    parser.add_argument(
        "--use-testpypi",
        required=False,
        default=False,
        action="store_true",
        help="Use testpypi for testing pip installation")
    parser.add_argument(
        "--use-localpypiartifact",
        required=False,
        default=None,
        help="Directory path where the downloaded pypi artifacts are present. " +
            "It should have two files: e.g. delta-spark-3.1.0.tar.gz, delta_spark-3.1.0-py3-none-any.whl")
    parser.add_argument(
        "--use-local",
        required=False,
        default=False,
        action="store_true",
        help="Generate JARs from local source code and use to run tests")
    parser.add_argument(
        "--run-storage-s3-dynamodb-integration-tests",
        required=False,
        default=False,
        action="store_true",
        help="Run the DynamoDB integration tests (and only them)")
    parser.add_argument(
        "--packages",
        required=False,
        default=None,
        help="Additional packages required for integration tests")
    parser.add_argument(
        "--dbb-conf",
        required=False,
        default=None,
        nargs="+",
        help="All `--conf` values passed to `spark-submit` for DynamoDB logstore/commit-coordinator integration tests")
    parser.add_argument(
        "--run-dynamodb-commit-coordinator-integration-tests",
        required=False,
        default=False,
        action="store_true",
        help="Run the DynamoDB Commit Coordinator tests (and only them)")
    parser.add_argument(
        "--run-iceberg-integration-tests",
        required=False,
        default=False,
        action="store_true",
        help="Run the Iceberg integration tests (and only them)")
    parser.add_argument(
        "--run-uniform-hudi-integration-tests",
        required=False,
        default=False,
        action="store_true",
        help="Run the Uniform Hudi integration tests (and only them)")
    parser.add_argument(
        "--iceberg-spark-version",
        required=False,
        default="4.0",
        help="Spark version for the Iceberg library (used in non-local mode)")
    parser.add_argument(
        "--iceberg-lib-version",
        required=False,
        default="1.4.0",
        help="Iceberg Spark Runtime library version")
    parser.add_argument(
        "--hudi-spark-version",
        required=False,
        default="4.0",
        help="Spark version for the Hudi library (used in non-local mode)")
    parser.add_argument(
        "--hudi-version",
        required=False,
        default="0.15.0",
        help="Hudi library version"
    )
    parser.add_argument(
        "--unity-catalog-commit-coordinator-integration-tests",
        required=False,
        default=False,
        action="store_true",
        help="Run the Unity Catalog Commit Coordinator tests (and only them)"
    )

    args = parser.parse_args()

    if args.scala_version not in ["2.13"]:
        raise Exception("Scala version can only be specified as --scala-version 2.13")

    if args.pip_only and args.no_pip:
        raise Exception("Cannot specify both --pip-only and --no-pip")

    if args.use_local and (args.version != default_version):
        raise Exception("Cannot specify --use-local with a --version different than in version.sbt")

    # When --use-local, publish all artifact variants once upfront and build the variant list
    # from CrossSparkVersions.scala. In non-local mode, use a single default (unsuffixed) variant.
    default_variant = {
        "suffix": "", "spark_version": "", "support_iceberg": "false", "support_hudi": "false"
    }
    spark_specs = None
    variants = [default_variant]
    if args.use_local:
        spark_specs = load_spark_version_specs(root_dir)
        clear_artifact_cache()
        publish_all_variants(root_dir, spark_specs)
        variants = get_spark_variants(spark_specs)

    run_python = not args.scala_only and not args.pip_only
    run_scala = not args.python_only and not args.pip_only
    run_pip = not args.python_only and not args.scala_only and not args.no_pip

    if args.run_iceberg_integration_tests:
        # In local mode, only test variants that support Iceberg.
        # In non-local mode, run once with --iceberg-spark-version from CLI args.
        if spark_specs:
            iceberg_variants = [v for v in variants if v["support_iceberg"] == "true"]
            if not iceberg_variants:
                print("No Spark variants support Iceberg - skipping Iceberg integration tests")
                quit()
        else:
            iceberg_variants = [{
                "suffix": "", "spark_version": args.iceberg_spark_version,
                "support_iceberg": "true", "support_hudi": "false"
            }]
        for variant in iceberg_variants:
            set_spark_env(variant["spark_version"])
            run_iceberg_integration_tests(
                root_dir, args.version, args.iceberg_lib_version, args.maven_repo, variant)
        quit()

    if args.run_uniform_hudi_integration_tests:
        # Build hudi assembly once before running tests (needs specific Spark version)
        if args.use_local:
            hudi_spark_ver = None
            if spark_specs:
                for spec in spark_specs:
                    if spec.get("supportHudi", "false") == "true":
                        hudi_spark_ver = spec["fullVersion"]
                        break
            if hudi_spark_ver:
                run_cmd(["build/sbt", "-DsparkVersion=%s" % hudi_spark_ver, "hudi/assembly"],
                        stream_output=True)
            else:
                run_cmd(["build/sbt", "hudi/assembly"], stream_output=True)

        # In local mode, only test variants that support Hudi.
        # In non-local mode, run once with --hudi-spark-version from CLI args.
        if spark_specs:
            hudi_variants = [v for v in variants if v["support_hudi"] == "true"]
            if not hudi_variants:
                print("No Spark variants support Hudi - skipping Hudi integration tests")
                quit()
        else:
            hudi_variants = [{
                "suffix": "", "spark_version": args.hudi_spark_version,
                "support_iceberg": "false", "support_hudi": "true"
            }]
        for variant in hudi_variants:
            set_spark_env(variant["spark_version"])
            run_uniform_hudi_integration_tests(
                root_dir, args.version, args.hudi_version, args.maven_repo, variant)
        quit()

    if args.run_storage_s3_dynamodb_integration_tests:
        for variant in variants:
            set_spark_env(variant["spark_version"])
            run_dynamodb_logstore_integration_tests(root_dir, args.version, args.test,
                                                    args.maven_repo, args.packages,
                                                    args.dbb_conf, variant)
        quit()

    if args.run_dynamodb_commit_coordinator_integration_tests:
        for variant in variants:
            set_spark_env(variant["spark_version"])
            run_dynamodb_commit_coordinator_integration_tests(root_dir, args.version, args.test,
                                                        args.maven_repo, args.packages,
                                                        args.dbb_conf, variant)
        quit()

    if args.s3_log_store_util_only:
        run_s3_log_store_util_integration_tests()
        quit()

    if args.unity_catalog_commit_coordinator_integration_tests:
        for variant in variants:
            set_spark_env(variant["spark_version"])
            run_unity_catalog_commit_coordinator_integration_tests(root_dir, args.version,
                                                                    args.test, variant,
                                                                    args.packages)
        quit()

    # Run the standard test suite: Scala, Python, pip
    # Each test function is called once per variant (the loop is here, not inside the functions)
    if run_scala:
        for variant in variants:
            set_spark_env(variant["spark_version"])
            run_scala_integration_tests(root_dir, args.version, args.test, args.maven_repo,
                                        args.scala_version, variant)

    if run_python:
        for variant in variants:
            set_spark_env(variant["spark_version"])
            run_python_integration_tests(root_dir, args.version, args.test, args.maven_repo,
                                         variant)

        test_missing_delta_storage_jar(root_dir, args.version, args.use_local)

    if run_pip:
        if args.use_testpypi and args.use_localpypiartifact is not None:
            raise Exception("Cannot specify both --use-testpypi and --use-localpypiartifact.")

        run_pip_installation_tests(root_dir, args.version, args.use_testpypi,
                                   args.use_localpypiartifact, args.maven_repo)
