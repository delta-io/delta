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

import argparse
import os
import glob
import re
import subprocess
import shlex
import shutil
from os import path

iceberg_lib_dir_name = "lib"
iceberg_src_dir_name = "iceberg_src" # this is a git dir
iceberg_patches_dir_name = "iceberg_src_patches"

iceberg_src_commit_hash = "ede085d0f7529f24acd0c81dd0a43f7bb969b763"
iceberg_src_branch = "main"  # only this branch will be downloaded

# Relative to iceberg_src directory.
# We use * because after applying the patches, a random git hash will be appended to each jar name.
# This, for all usages below, we must search for these jar files using `glob.glob(pattern)`
iceberg_src_compiled_jar_rel_glob_patterns = [
    "bundled-guava/build/libs/iceberg-bundled-guava-*.jar",
    "common/build/libs/iceberg-common-*.jar",
    "api/build/libs/iceberg-api-*.jar",
    "core/build/libs/iceberg-core-*.jar",
    "parquet/build/libs/iceberg-parquet-*.jar",
    "hive-metastore/build/libs/iceberg-hive-*.jar",
    "data/build/libs/iceberg-data-*.jar"
]

iceberg_root_dir = path.abspath(path.dirname(__file__)) # this is NOT a git dir
iceberg_src_dir = path.join(iceberg_root_dir, iceberg_src_dir_name)
iceberg_patches_dir = path.join(iceberg_root_dir, iceberg_patches_dir_name)
iceberg_lib_dir = path.join(iceberg_root_dir, iceberg_lib_dir_name)


def iceberg_jars_exists():
    for compiled_jar_rel_glob_pattern in iceberg_src_compiled_jar_rel_glob_patterns:
        jar_file_name_pattern = path.basename(path.normpath(compiled_jar_rel_glob_pattern))
        lib_jar_abs_pattern = path.join(iceberg_lib_dir, jar_file_name_pattern)
        results = glob.glob(lib_jar_abs_pattern)

        if len(results) > 1:
            raise Exception("More jars than expected: " + str(results))
        
        if len(results) == 0:
            return False

    return True


def prepare_iceberg_source():
    with WorkingDirectory(iceberg_root_dir):
        print(">>> Cloning Iceberg repo")
        shutil.rmtree(iceberg_src_dir_name, ignore_errors=True)

        # We just want the shallowest, smallest iceberg clone. We will check out the commit later.
        run_cmd("git clone --depth 1 --branch %s https://github.com/apache/iceberg.git %s" %
                (iceberg_src_branch, iceberg_src_dir_name))

    with WorkingDirectory(iceberg_src_dir):
        run_cmd("git config user.email \"<>\"")
        run_cmd("git config user.name \"Anonymous\"")

        # Fetch just the single commit (shallow)
        run_cmd("git fetch origin %s --depth 1" % iceberg_src_commit_hash)
        run_cmd("git checkout %s" % iceberg_src_commit_hash)

        print(">>> Applying patch files")
        patch_files = glob.glob(path.join(iceberg_patches_dir, "*.patch"))
        patch_files.sort()

        for patch_file in patch_files:
            print(">>> Applying '%s'" % patch_file)
            run_cmd("git apply %s" % patch_file)
            run_cmd("git add .")
            run_cmd("git commit -a -m 'applied %s'" % path.basename(patch_file))


_KEPT_PROJECTS = {
    "iceberg-bundled-guava", "iceberg-api", "iceberg-common",
    "iceberg-core", "iceberg-data", "iceberg-orc",
    "iceberg-parquet", "iceberg-hive-metastore",
}


def _make_maven_block(url, label, jfrog_token=None):
    """Build a Gradle maven { ... } block, optionally with credentials."""
    if jfrog_token:
        return (
            'maven {\n'
            '      url "%s"\n'
            '      credentials {\n'
            '        username "gha-service-account"\n'
            '        password "%s"\n'
            '      }\n'
            '    }' % (url, jfrog_token)
        )
    return 'maven { url "%s" }' % url


def _strip_unwanted_project_blocks(content):
    """Remove top-level project(':iceberg-X') { ... } blocks for projects not in _KEPT_PROJECTS."""
    lines = content.split("\n")
    result = []
    skip_depth = 0
    skipping = False

    for line in lines:
        if not skipping:
            m = re.match(r"^project\(':([^']+)'\)\s*\{", line)
            if m and m.group(1) not in _KEPT_PROJECTS:
                skipping = True
                skip_depth = 1
                continue
            result.append(line)
        else:
            skip_depth += line.count("{") - line.count("}")
            if skip_depth <= 0:
                skipping = False

    return "\n".join(result)


def patch_iceberg_build_for_proxies():
    """Patch Iceberg build files for restricted-egress environments.

    Reads optional env vars GRADLE_PROXY_URL, MAVEN_PROXY_URL, and
    JFROG_ACCESS_TOKEN to redirect Gradle plugin/dependency resolution
    through proxy mirrors. When none are set, repos stay at their
    upstream defaults.

    Also trims settings.gradle to only the sub-projects we need,
    relaxes the JDK version gate, and strips the palantir-baseline
    plugin (blocked on common proxies).
    """
    gradle_proxy = os.environ.get("GRADLE_PROXY_URL")
    maven_proxy = os.environ.get("MAVEN_PROXY_URL")
    jfrog_token = os.environ.get("JFROG_ACCESS_TOKEN")

    # --- 1. Patch build.gradle ---
    build_gradle = path.join(iceberg_src_dir, "build.gradle")
    with open(build_gradle, "r") as f:
        content = f.read()

    if gradle_proxy:
        print(">>> Patching Gradle plugin repo: GRADLE_PROXY_URL=%s" % gradle_proxy)
        block = _make_maven_block(gradle_proxy, "gradle-proxy", jfrog_token)
        content = content.replace("gradlePluginPortal()", block)

        # The plugins { id '...' } block resolves through pluginManagement
        # repos which default to the Gradle Plugin Portal — unreachable in
        # proxy environments. Move nebula to the buildscript classpath and
        # use apply plugin instead.
        content = re.sub(r"plugins\s*\{[^}]*\}", 'apply plugin: "nebula.dependency-recommender"', content)
        content = content.replace(
            "classpath 'com.palantir.gradle.gitversion:gradle-git-version:0.15.0'",
            "classpath 'com.palantir.gradle.gitversion:gradle-git-version:0.15.0'\n"
            "    classpath 'com.netflix.nebula:nebula-dependency-recommender:11.0.0'"
        )
        print(">>> Moved nebula plugin from plugins{} to buildscript classpath")
    else:
        print(">>> GRADLE_PROXY_URL not set — using upstream gradlePluginPortal()")

    if maven_proxy:
        print(">>> Patching Maven Central repo: MAVEN_PROXY_URL=%s" % maven_proxy)
        block = _make_maven_block(maven_proxy, "maven-proxy", jfrog_token)
        content = content.replace("mavenCentral()", block)
    else:
        print(">>> MAVEN_PROXY_URL not set — using upstream mavenCentral()")

    # Strip palantir-baseline classpath (403 on common proxies)
    content = re.sub(
        r"^\s*classpath\s+'com\.palantir\.baseline:gradle-baseline-java:.*'\n",
        "",
        content,
        flags=re.MULTILINE,
    )
    print(">>> Stripped palantir-baseline classpath entry")

    # Replace `apply from: 'baseline.gradle'` with a comment
    content = re.sub(
        r"^(\s*)apply from:\s*'baseline\.gradle'",
        r"\1// stripped: baseline.gradle",
        content,
        flags=re.MULTILINE,
    )
    print(">>> Commented out baseline.gradle apply")

    # Relax JDK version gate: replace the throw with a fallback assignment
    content = re.sub(
        r"throw new GradleException\(\"This build must be run with JDK 8 or 11.*?\"\s*\+\s*JavaVersion\.current\(\)\)",
        "project.ext.jdkVersion = JavaVersion.current().toString()",
        content,
    )
    print(">>> Relaxed JDK version check in build.gradle")

    content = _strip_unwanted_project_blocks(content)
    print(">>> Stripped project blocks for unused sub-projects")

    with open(build_gradle, "w") as f:
        f.write(content)

    # --- 2. Rewrite jmh.gradle ---
    # The original references spark/flink sub-projects we no longer include.
    # Write a minimal version that only configures JMH for kept projects.
    jmh_gradle = path.join(iceberg_src_dir, "jmh.gradle")
    minimal_jmh = """\
if (jdkVersion != '8' && jdkVersion != '11') {
  project.logger.warn('Skipping JMH JDK check \\u2014 running on JDK ' + JavaVersion.current())
}

def jmhProjects = [project(":iceberg-core"), project(":iceberg-data")]

configure(jmhProjects) {
  apply plugin: 'me.champeau.jmh'

  jmh {
    jmhVersion = '1.32'
    failOnError = true
    forceGC = true
    includeTests = true
    humanOutputFile = file(jmhOutputPath)
    includes = [jmhIncludeRegex]
    zip64 = true
  }

  jmhCompileGeneratedClasses {
    pluginManager.withPlugin('com.palantir.baseline-error-prone') {
      options.errorprone.enabled = false
    }
  }
}
"""
    with open(jmh_gradle, "w") as f:
        f.write(minimal_jmh)
    print(">>> Rewrote jmh.gradle for trimmed sub-project set")

    # --- 3. Trim settings.gradle ---
    settings_gradle = path.join(iceberg_src_dir, "settings.gradle")
    trimmed_settings = """\
rootProject.name = 'iceberg'
include 'api'
include 'common'
include 'core'
include 'data'
include 'orc'
include 'parquet'
include 'bundled-guava'
include 'hive-metastore'

project(':api').name = 'iceberg-api'
project(':common').name = 'iceberg-common'
project(':core').name = 'iceberg-core'
project(':data').name = 'iceberg-data'
project(':orc').name = 'iceberg-orc'
project(':parquet').name = 'iceberg-parquet'
project(':bundled-guava').name = 'iceberg-bundled-guava'
project(':hive-metastore').name = 'iceberg-hive-metastore'
"""
    with open(settings_gradle, "w") as f:
        f.write(trimmed_settings)
    print(">>> Trimmed settings.gradle to required sub-projects only")


def generate_iceberg_jars():
    print(">>> Compiling JARs")
    with WorkingDirectory(iceberg_src_dir):
        build_args = "-x test -x javadoc -x testJar"
        run_cmd("./gradlew :iceberg-core:build %s" % build_args)
        run_cmd("./gradlew :iceberg-parquet:build %s" % build_args)
        run_cmd("./gradlew :iceberg-hive-metastore:build %s" % build_args)
        run_cmd("./gradlew :iceberg-data:build %s" % build_args)

    print(">>> Copying JARs to lib directory")
    shutil.rmtree(iceberg_lib_dir, ignore_errors=True)
    os.mkdir(iceberg_lib_dir)

    # For each relative pattern p ...
    for compiled_jar_rel_glob_pattern in iceberg_src_compiled_jar_rel_glob_patterns:
        # Get the absolute pattern
        compiled_jar_abs_pattern = path.join(iceberg_src_dir, compiled_jar_rel_glob_pattern)
        # Search for all glob results
        results = glob.glob(compiled_jar_abs_pattern)
        # Compiled jars will include tests, sources, javadocs; exclude them
        results = list(filter(lambda result: all(x not in result for x in ["tests.jar", "sources.jar", "javadoc.jar"]), results))

        if len(results) == 0:
            raise Exception("Could not find the jar: " + compled_jar_rel_glob_pattern)
        if len(results) > 1:
            raise Exception("More jars created than expected: " + str(results))

        # Copy the one jar result into the <iceberg root>/lib directory
        compiled_jar_abs_path = results[0]
        compiled_jar_name = path.basename(path.normpath(compiled_jar_abs_path))
        lib_jar_abs_path = path.join(iceberg_lib_dir, compiled_jar_name)
        shutil.copyfile(compiled_jar_abs_path, lib_jar_abs_path)

    if not iceberg_jars_exists():
        raise Exception("JAR copying failed")


def run_cmd(cmd, throw_on_error=True, env=None, stream_output=False, **kwargs):
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    cmd_env = os.environ.copy()
    if env:
        cmd_env.update(env)

    if stream_output:
        child = subprocess.Popen(cmd, env=cmd_env, **kwargs)
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception("Non-zero exitcode: %s" % (exit_code))
        print("----\n")
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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force",
        required=False,
        default=False,
        action="store_true",
        help="Force the generation even if already generated, useful for testing.")
    args = parser.parse_args()

    if args.force or not iceberg_jars_exists():
        prepare_iceberg_source()
        patch_iceberg_build_for_proxies()
        generate_iceberg_jars()
