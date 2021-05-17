#
# Copyright (2020) The Delta Lake Project Authors.
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

from pyspark.sql import SparkSession
import delta


class PipUtilsTests(unittest.TestCase):

    def setUp(self):
        builder = SparkSession.builder \
            .appName("pip-test") \
            .master("local[*]") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        self.spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
        self.tempPath = tempfile.mkdtemp()
        self.tempFile = os.path.join(self.tempPath, "tempFile")

    def tearDown(self):
        self.spark.stop()
        shutil.rmtree(self.tempPath)

    def test_maven_jar_loaded(self):
        # Read and write Delta table to check that the maven jars are loaded and Delta works.
        self.spark.range(0, 5).write.format("delta").save(self.tempFile)
        self.spark.read.format("delta").load(self.tempFile)
