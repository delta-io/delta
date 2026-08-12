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
import shutil
import tempfile
import unittest
from typing import List, Optional
from unittest.mock import patch

from pyspark.sql import SparkSession
import delta
from delta.pip_utils import _python_to_maven_version


class PipUtilsVersionTests(unittest.TestCase):

    def test_python_to_maven_version(self) -> None:
        test_cases = {
            "4.4.0": "4.4.0",
            "4.4.0.dev0": "4.4.0-SNAPSHOT",
            "4.4.0rc1": "4.4.0-rc1",
            "4.4.0rc1.dev0": "4.4.0-rc1-SNAPSHOT",
            "4.4.0rc1.dev1": "4.4.0-rc1-SNAPSHOT",
            "4.4.0.post1": "4.4.0.post1",
        }

        for python_version, expected_maven_version in test_cases.items():
            with self.subTest(python_version=python_version):
                self.assertEqual(
                    _python_to_maven_version(python_version),
                    expected_maven_version)

    @patch("importlib_metadata.version", return_value="4.4.0rc1.dev0")
    @patch("pyspark.__version__", "4.2.0")
    def test_configure_uses_maven_version(self, _) -> None:
        builder = delta.configure_spark_with_delta_pip(SparkSession.builder)

        self.assertEqual(
            builder._options["spark.jars.packages"],
            "io.delta:delta-spark_4.2_2.13:4.4.0-rc1-SNAPSHOT")


class PipUtilsTests(unittest.TestCase):

    def setUp(self) -> None:
        builder = SparkSession.builder \
            .appName("pip-test") \
            .master("local[*]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        self.spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
        self.tempPath = tempfile.mkdtemp()
        self.tempFile = os.path.join(self.tempPath, "tempFile")

    def tearDown(self) -> None:
        self.spark.stop()
        shutil.rmtree(self.tempPath)

    def test_maven_jar_loaded(self) -> None:
        # Read and write Delta table to check that the maven jars are loaded and Delta works.
        self.spark.range(0, 5).write.format("delta").save(self.tempFile)
        self.spark.read.format("delta").load(self.tempFile)


class PipUtilsCustomJarsTests(unittest.TestCase):

    def setUp(self) -> None:
        builder = SparkSession.builder \
            .appName("pip-test") \
            .master("local[*]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        import importlib_metadata
        scala_version = "2.12"
        delta_version = importlib_metadata.version("delta_spark")
        maven_artifacts = [f"io.delta:delta-spark_{scala_version}:{delta_version}"]
        # configure extra packages
        self.spark = delta.configure_spark_with_delta_pip(builder, maven_artifacts).getOrCreate()

        self.tempPath = tempfile.mkdtemp()
        self.tempFile = os.path.join(self.tempPath, "tempFile")

    def tearDown(self) -> None:
        self.spark.stop()
        shutil.rmtree(self.tempPath)

    def test_maven_jar_loaded(self) -> None:
        packagesConf: Optional[str] = self.spark.conf.get("spark.jars.packages")
        assert packagesConf is not None  # mypi needs this to assign type str from Optional[str]
        packages: str = packagesConf
        packagesList: List[str] = packages.split(",")
        # Check `spark.jars.packages` contains `extra_packages`
        self.assertTrue(len(packagesList) == 2, "There should only be 2 packages")

        # Read and write Delta table to check that the maven jars are loaded and Delta works.
        self.spark.range(0, 5).write.format("delta").save(self.tempFile)
        self.spark.read.format("delta").load(self.tempFile)


if __name__ == "__main__":
    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=4)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=4)
