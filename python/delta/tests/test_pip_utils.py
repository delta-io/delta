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
from typing import List

from pyspark.sql import SparkSession
import delta


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
        maven_artifacts = [f"io.delta:delta-core_{scala_version}:{delta_version}"]
        # configure extra packages
        self.spark = delta.configure_spark_with_delta_pip(builder, maven_artifacts).getOrCreate()

        self.tempPath = tempfile.mkdtemp()
        self.tempFile = os.path.join(self.tempPath, "tempFile")

    def tearDown(self) -> None:
        self.spark.stop()
        shutil.rmtree(self.tempPath)

    def test_maven_jar_loaded(self) -> None:
        packages: List[str] = self.spark.conf.get("spark.jars.packages").split(",")

        # Check `spark.jars.packages` contains `extra_packages`
        self.assertTrue(len(packages) == 2, "There should only be 2 packages")

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
